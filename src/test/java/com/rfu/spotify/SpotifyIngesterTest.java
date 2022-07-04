package com.rfu.spotify;

import com.opencsv.exceptions.CsvException;
import com.opencsv.exceptions.CsvValidationException;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.common.SolrInputDocument;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

/**
 * Tests for SpotifyIngester.
 */
public class SpotifyIngesterTest {

  SpotifyIngester sp;

  @BeforeEach
  public void setUp() {
    sp = new SpotifyIngester();
  }

  /**
   * Tests the behavior of the uploadCSV method.
   *
   * @throws SolrServerException
   * @throws IOException
   * @throws CsvException
   */
  @Test
  public void testUploadCSVToSolr() throws SolrServerException, IOException, CsvException {
    SolrClient solrClient = mock(SolrClient.class);

    ArgumentCaptor<String> collectionCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<List<SolrInputDocument>> documentsCaptor = ArgumentCaptor.forClass(List.class);

    sp.uploadCSV(solrClient);

    // verify that client adds 370 batches of 50 documents each
    verify(solrClient, times(370)).add(collectionCaptor.capture(), documentsCaptor.capture());
    String collection = collectionCaptor.getValue();

    // verify that the collection that is uploaded to is correct
    assertEquals(collection, "spotify_dataset");

    List<SolrInputDocument> documents = documentsCaptor.getValue();

    // verify that there are 4 uncleared documents left by the end of the upload
    assertEquals(documents.size(), 4);

    // verify that the last document left is the same as the last document in the csv
    assertEquals(documents.get(3).get("track_name").toString(), "track_name=Migraine");
    assertEquals(documents.get(3).get("id").toString(), "id=18454");
  }

  /**
   * Tests the behavior of the stream method.
   *
   * @throws IOException
   */
  @Test
  public void testStream() throws IOException {
    TupleStream stream = mock(TupleStream.class);

    sp.stream(stream);

    verify(stream, times(1)).open();
    verify(stream, times(1)).close();
  }

  /**
   * Tests the cleanDate method.
   */
  @Test
  public void testCleanDate() {
    // Testing input dates without months and days
    Assertions.assertEquals(sp.cleanDate("1967"), "1967-01-01");
    Assertions.assertEquals(sp.cleanDate("1422"), "1422-01-01");
    // Testing input dates without days
    Assertions.assertEquals(sp.cleanDate("1111-04"), "1111-04-01");
    Assertions.assertEquals(sp.cleanDate("2011-09"), "2011-09-01");
    // Testing input dates that are normal
    Assertions.assertEquals(sp.cleanDate("2002-11-22"), "2002-11-22");
    Assertions.assertEquals(sp.cleanDate("2022-01-20"), "2022-01-20");
  }

  /**
   * Tests the cleanDate method with a null String.
   */
  @Test
  public void testNullDate() {
    assertThrows(IllegalArgumentException.class, () -> sp.cleanDate(null));
  }

  @Test
  public void testUploadCSVToElastic() throws CsvValidationException, IOException {
    RestHighLevelClient mockClient = mock(RestHighLevelClient.class);

    ArgumentCaptor<BulkRequest> bulkRequestCaptor = ArgumentCaptor.forClass(BulkRequest.class);
    ArgumentCaptor<RequestOptions> requestOptionsCaptor = ArgumentCaptor.forClass(RequestOptions.class);

    sp.uploadCSV(mockClient);
    verify(mockClient, times(370)).bulk(bulkRequestCaptor.capture(), requestOptionsCaptor.capture());
  }

  @Test
  public void testNullTrackName() {
    assertThrows(IllegalArgumentException.class, () -> sp.getDenseVector(null));
  }

  @Test
  public void testComputeCosineSimilarity() {
    double[] v1 = new double[]{0.33, 0, 42, 0.11, 0.54, 0.95};
    double[] v2 = new double[]{0.33, 0, 42, 0.11, 0.54, 0.95};
    assertEquals(sp.computeCosineSimilarity(v1, v2), 1);
  }

  @Test
  public void testInvalidVectors() {
    double[] v1 = new double[]{0.33, 0, 42, 0.11, 0.54, 0.95};
    double[] v2 = null;
    // test null vectors as arguments
    assertThrows(IllegalArgumentException.class, () -> sp.computeCosineSimilarity(v1, v2));
    assertThrows(IllegalArgumentException.class, () -> sp.computeCosineSimilarity(v2, v1));

    double[] v3 = new double[]{0.2, 0.5};
    // test invalid dimensions
    assertThrows(IllegalArgumentException.class, () -> sp.computeCosineSimilarity(v1, v3));
    assertThrows(IllegalArgumentException.class, () -> sp.computeCosineSimilarity(v3, v1));
  }

  @Test
  public void testGetDenseVectorNullTrackName() {
    assertThrows(IllegalArgumentException.class, () -> sp.getDenseVector(null));
  }
}
