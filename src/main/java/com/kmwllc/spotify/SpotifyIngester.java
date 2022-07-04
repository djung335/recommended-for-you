package com.rfu.spotify;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import com.opencsv.exceptions.CsvValidationException;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.http.HttpHost;
import org.apache.log4j.BasicConfigurator;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.PutMappingRequest;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScriptScoreFunctionBuilder;
import org.elasticsearch.index.query.functionscore.ScriptScoreQueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * This class holds the main method to ingest Spotify data.
 */
public class SpotifyIngester {

  private static final Logger log = LoggerFactory.getLogger(SpotifyIngester.class);


  public SpotifyIngester() {
  }

  /**
   * Cleans the date by putting it in the following format: YYYY-MM-DD
   * Example: 2002 => 2002-01-01
   *
   * @param inputDate the date to be cleaned
   * @return the cleaned date
   * @throws IllegalArgumentException if the date is null
   */
  public static String cleanDate(String inputDate) throws IllegalArgumentException {
    if (inputDate == null) {
      throw new IllegalArgumentException("input date cannot be null");
    } else if (inputDate.length() <= 4) {
      return inputDate + "-01-01";
    } else if (inputDate.length() <= 7) {
      return inputDate + "-01";
    } else {
      return inputDate;
    }
  }

  /**
   * Uploads the csv file to a Solr collection in batches of 50
   *
   * @throws IOException
   * @throws SolrServerException
   * @throws CsvException
   */
  public static void uploadCSV(SolrClient client) throws IOException, CsvException {
    CSVReader reader = new CSVReader(new FileReader("src/main/resources/spotify_songs.csv"));

    // parse the first input in order to get the fields
    String[] nextLine = reader.readNext();
    List<String> fields = new ArrayList<String>();
    for (String field : nextLine) {
      // iterate through the first document and grab all the field names
      fields.add(field);
    }

    List<SolrInputDocument> documents = new ArrayList<SolrInputDocument>();
    int csvLineNumber = 1;
    while ((nextLine = reader.readNext()) != null) {
      SolrInputDocument document = new SolrInputDocument();
      for (int i = 0; i < nextLine.length; i++) {
        if (fields.get(i).equals("track_album_release_date")) {
          document.addField("track_album_release_date", cleanDate(nextLine[i]));
        } else {
          document.addField(fields.get(i), nextLine[i]);
        }
      }

      document.addField("id", csvLineNumber);
      csvLineNumber++;
      documents.add(document);

      // upload in batches of 50
      if (documents.size() == 50) {
        try {
          client.add("spotify_dataset", documents);
        } catch (Exception e) {
          //System.out.println("ingestion was invalid");
          log.error("ingestion was invalid", e);
        }
        documents.clear();
      }
    }

    // if there are any documents left over, add them
    try {
      client.add("spotify_dataset", documents);
    } catch (SolrServerException e) {
      log.error("ingestion was invalid", e);
    }
  }

  /**
   * Uploads the CSV file to Solr and tracks the time elapsed.
   *
   * @throws IOException
   * @throws CsvException
   */
  public static void uploadCSVToSolr() throws IOException, CsvException {
    StopWatch watch = new StopWatch();
    watch.start();

    String SOLR_URL = "http://localhost:8983/solr";
    SolrClient client = new HttpSolrClient.Builder(SOLR_URL).build();
    uploadCSV(client);

    watch.stop();

    // Print out the elapsed time in ms
    log.info("Time Elapsed: " + watch.getTime() + " ms");
  }

  /**
   * Uploads the CSV using Elasticsearch's RestHighLevelClient.
   *
   * @param client Elasticsearch's RestHighLevelClient
   * @throws IOException
   * @throws CsvValidationException
   */
  public static void uploadCSV(RestHighLevelClient client) throws IOException, CsvValidationException {
    // create a mapping for a dense vector
    PutMappingRequest putMappingRequest = new PutMappingRequest("spotify_songs");

    Map<String, Object> denseVector = new HashMap<String, Object>();
    denseVector.put("type", "dense_vector");
    denseVector.put("dims", 7);

    Map<String, Object> properties = new HashMap<String, Object>();
    properties.put("dense_vector", denseVector);

    Map<String, Object> mappings = new HashMap<String, Object>();
    mappings.put("properties", properties);

    putMappingRequest.source(mappings);
    client.indices().putMapping(putMappingRequest, RequestOptions.DEFAULT);

    CSVReader reader = new CSVReader(new FileReader("src/main/resources/spotify_songs.csv"));

    // parse the first input in order to get the fields
    String[] nextLine = reader.readNext();
    List<String> fields = new ArrayList<String>();
    for (String field : nextLine) {
      // iterate through the first document and grab all the field names
      fields.add(field);
    }

    BulkRequest bulkRequest = new BulkRequest("spotify_songs");

    int csvLineNumber = 1;

    while ((nextLine = reader.readNext()) != null) {
      Map<String, Object> jsonMap = new HashMap<String, Object>();

      for (int i = 0; i < nextLine.length; i++) {
        jsonMap.put(fields.get(i), nextLine[i]);
      }

      // add dense vector
      double[] v = new double[7];
      v[0] = Double.valueOf(jsonMap.get("danceability").toString());
      v[1] = Double.valueOf(jsonMap.get("energy").toString());
      v[2] = Double.valueOf(jsonMap.get("speechiness").toString());
      v[3] = Double.valueOf(jsonMap.get("acousticness").toString());
      v[4] = Double.valueOf(jsonMap.get("instrumentalness").toString());
      v[5] = Double.valueOf(jsonMap.get("valence").toString());
      v[6] = Double.valueOf(jsonMap.get("mode").toString()) / 5;

      jsonMap.put("dense_vector", v);

      bulkRequest.add(new IndexRequest("spotify_songs").id(Integer.toString(csvLineNumber)).source(jsonMap));

      csvLineNumber++;

      if (bulkRequest.numberOfActions() == 50) {
        client.bulk(bulkRequest, RequestOptions.DEFAULT);
        bulkRequest = new BulkRequest();
      }
    }

    // If there are any remaining documents, add them
    client.bulk(bulkRequest, RequestOptions.DEFAULT);

    log.info("Data successfully ingested into Elasticsearch");
    client.close();
  }

  /**
   * Uploads the CSV to Elasticsearch and prints the time elapsed.
   *
   * @throws CsvValidationException
   * @throws IOException
   */
  public static void uploadCSVToElastic() throws CsvValidationException, IOException {
    log.info("Starting ingestion to Elasticsearch");
    StopWatch watch = new StopWatch();
    watch.start();

    RestClientBuilder httpClientBuilder = RestClient.builder(new HttpHost("localhost", 9200));
    RestHighLevelClient client = new RestHighLevelClient(httpClientBuilder);
    uploadCSV(client);

    watch.stop();
    System.out.println("Time Elapsed: " + watch.getTime());
  }

  /**
   * Makes queries to the Spotify Dataset collection through Solr
   *
   * @throws SolrServerException
   * @throws IOException
   */
  public static void query(SolrClient client, SolrQuery query) throws SolrServerException, IOException {
    // Query 1 : return all documents
    query.add("q", "*:*");

    QueryResponse resp = client.query("spotify_dataset", query);
    SolrDocumentList documents = resp.getResults();

    for (SolrDocument d : documents) {
      System.out.println(d.toString());
    }
  }

  /**
   * Evaluates a streaming expression request to Solr
   *
   * @throws IOException
   */
  public static void stream(TupleStream stream) throws IOException {
    // the question being answered: what 5 playlist genres had the largest increase in songs added between 2010-2015 and 2015-2020?
    stream.setStreamContext(new StreamContext());
    stream.open();
    if (stream._size() == 0) {
      System.out.println("Invalid expression");
    } else {
      for (int i = 0; i < 5; i++) {
        Tuple tp = stream.read();
        System.out.println(tp.get("playlist_genre") + ": " + tp.get("change") + "%");
      }
    }
    stream.close();
  }

  /**
   * Makes queries to the Spotify dataset through Elasticsearch.
   *
   * @throws IOException
   */
  public static void queryElasticsearch() throws IOException {
    RestClientBuilder httpClientBuilder = RestClient.builder(new HttpHost("localhost", 9200));
    RestHighLevelClient hlrc = new RestHighLevelClient(httpClientBuilder);

    GetRequest getRequest = new GetRequest("spotify_songs", "2");
    GetResponse getResponse = hlrc.get(getRequest, RequestOptions.DEFAULT);

    System.out.println(getResponse.toString());

    hlrc.close();
  }

  /**
   * Given a track name, retrieves the track's dense vector from Elasticsearch.
   *
   * @param trackName the name of the track
   * @return the dense vector as a Double array
   * @throws IOException
   * @throws IllegalArgumentException if the trackName is null
   */
  public static Double[] getDenseVector(String trackName) throws IOException, IllegalArgumentException {
    if (trackName == null) {
      throw new IllegalArgumentException("trackName cannot be null.");
    }

    RestClientBuilder httpClientBuilder = RestClient.builder(new HttpHost("localhost", 9200));
    RestHighLevelClient hlrc = new RestHighLevelClient(httpClientBuilder);

    MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("track_name", trackName);

    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.query(matchQueryBuilder);
    searchSourceBuilder.size(1);

    SearchRequest searchRequest = new SearchRequest("spotify_songs");
    searchRequest.source(searchSourceBuilder);

    SearchResponse searchResponse = hlrc.search(searchRequest, RequestOptions.DEFAULT);

    hlrc.close();

    SearchHits searchHits = searchResponse.getHits();
    Map<String, Object> map = searchHits.getAt(0).getSourceAsMap();

    List<Double> vector = (ArrayList) map.get("dense_vector");

    System.out.println(Arrays.toString(vector.toArray(new Double[0])));
    System.out.println("If you liked " + map.get("track_name") + " by " + map.get("track_artist") + "...");
    return vector.toArray(new Double[0]);
  }

  public static String getGenre(String trackName) throws IOException, IllegalArgumentException {
    if (trackName == null) {
      throw new IllegalArgumentException("trackName cannot be null.");
    }

    RestClientBuilder httpClientBuilder = RestClient.builder(new HttpHost("localhost", 9200));
    RestHighLevelClient hlrc = new RestHighLevelClient(httpClientBuilder);

    MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("track_name", trackName);

    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.query(matchQueryBuilder);
    searchSourceBuilder.size(1);

    SearchRequest searchRequest = new SearchRequest("spotify_songs");
    searchRequest.source(searchSourceBuilder);

    SearchResponse searchResponse = hlrc.search(searchRequest, RequestOptions.DEFAULT);

    hlrc.close();

    SearchHits searchHits = searchResponse.getHits();
    Map<String, Object> map = searchHits.getAt(0).getSourceAsMap();

    String genre = (String) map.get("playlist_genre");

    System.out.println(genre);

    return genre;
  }

  /**
   * Performs a vector search to find similar songs to the given track through cosine similarity.
   *
   * @param trackName name of the track
   * @throws IOException
   * @throws IllegalArgumentException if the track name is null
   */
  public static void vectorSearch(String trackName) throws IOException, IllegalArgumentException {
    RestClientBuilder httpClientBuilder = RestClient.builder(new HttpHost("localhost", 9200));
    RestHighLevelClient hlrc = new RestHighLevelClient(httpClientBuilder);

    Double[] vector = getDenseVector(trackName);
    String genre = getGenre(trackName);

//    QueryBuilder matchAllQueryBuilder = new MatchAllQueryBuilder();
    MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("playlist_genre", genre);

    Map<String, Object> params = new HashMap<String, Object>();
    params.put("query_vector", vector);
    Script script = new Script(Script.DEFAULT_SCRIPT_TYPE, "painless", "cosineSimilarity(params.query_vector, doc['dense_vector']) + 1.0", params);
    ScriptScoreFunctionBuilder scriptScoreFunctionBuilder = new ScriptScoreFunctionBuilder(script);

    ScriptScoreQueryBuilder scriptScoreQueryBuilder = new ScriptScoreQueryBuilder(matchQueryBuilder, scriptScoreFunctionBuilder);

    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    // specify which fields to see
    searchSourceBuilder.fetchSource(new String[]{"track_name", "track_artist", "dense_vector"}, null);
    searchSourceBuilder.query(scriptScoreQueryBuilder);

    SearchRequest searchRequest = new SearchRequest("spotify_songs");
    searchRequest.source(searchSourceBuilder);

    SearchResponse searchResponse = hlrc.search(searchRequest, RequestOptions.DEFAULT);

    hlrc.close();

    SearchHits searchHits = searchResponse.getHits();

    System.out.println("You might like...");

    for (SearchHit s : searchHits) {
      Map<String, Object> map = s.getSourceAsMap();
      System.out.println(map.get("track_name") + " by " + map.get("track_artist") + " : " + s.getScore());
    }
  }

  /**
   * Computes the cosine similarity score between two vectors.
   *
   * @param v1 vector one
   * @param v2 vector two
   * @return the cosine similarity score as a double
   * @throws IllegalArgumentException if arguments are null or the dimensions are not equal
   */
  public static double computeCosineSimilarity(double[] v1, double[] v2) throws IllegalArgumentException {
    if (v1 == null || v2 == null || v1.length != v2.length) {
      throw new IllegalArgumentException();
    } else {
      double mag1 = 0.0;
      double mag2 = 0.0;
      double dotProduct = 0.0;
      for (int i = 0; i < v1.length; i++) {
        mag1 = mag1 + Math.pow(v1[i], 2);
        mag2 = mag2 + Math.pow(v2[i], 2);
        dotProduct = dotProduct + v1[i] * v2[i];
      }
      mag1 = Math.sqrt(mag1);
      mag2 = Math.sqrt(mag2);

      return dotProduct / (mag1 * mag2);
    }
  }

  // args[0] = solr or elasticsearch
  public static void main(String[] args) throws IOException, CsvValidationException {
    BasicConfigurator.configure();
  }
}