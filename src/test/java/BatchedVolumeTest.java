/*
 * Copyright 2017 Splunk, Inc..
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.util.PropertiesFileHelper;

import java.net.URL;
import java.util.List;
import java.util.Properties;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.UUID;
import java.util.concurrent.TimeoutException;

/**
 *
 * @author ghendrey
 */
public class BatchedVolumeTest extends AbstractConnectionTest {

  protected int numToSend = 1000000;

  private String TEXT_TO_RAW_WITH_BUFFERING = "TEXT_TO_RAW_WITH_BUFFERING";
  private String JSON_TO_RAW_WITH_BUFFERING = "JSON_TO_RAW_WITH_BUFFERING";
  private String TEXT_TO_EVENTS_WITH_BUFFERING = "TEXT_TO_EVENTS_WITH_BUFFERING";
  private String JSON_TO_EVENTS_WITH_BUFFERING = "JSON_TO_EVENTS_WITH_BUFFERING";

  private String SINGLE_INSTANCE_LOCAL = "SINGLE_INSTANCE_LOCAL";
  private String LOCAL_CLUSTER = "LOCAL_CLUSTER";
  private String CLOUD_CLUSTER_WITH_ELB = "CLOUD_CLUSTER_WITH_ELB";
  private String CLOUD_CLUSTER_DIRECT_TO_INDEXERS = "CLOUD_CLUSTER_DIRECT_TO_INDEXERS";

  private String run_id = UUID.randomUUID().toString(); // All 4 tests in this suite will have the same run ID

  /* ************************************* SETTINGS **************************************** */
  private String splunkType = CLOUD_CLUSTER_DIRECT_TO_INDEXERS; // just a label - change this before every test run
  private String notes = "null"; // e.g. replication factor of 3
  private int bufferSize = 0; // 1024*16
  /* ************************************ /SETTINGS **************************************** */

  public BatchedVolumeTest() {
  }



  @Test
  public void sendTextToRawEndpointWithBuffering() throws InterruptedException, TimeoutException {
    connection.setHecEndpointType(Connection.HecEndpoint.RAW_EVENTS_ENDPOINT);
    connection.setTestName(TEXT_TO_RAW_WITH_BUFFERING);
    super.eventType = EventType.TEXT;
    configureConnectionForMetrics(connection);
    sendWithMetrics();
  }

    @Test
  public void sendJsonToRawEndpointWithBuffering() throws InterruptedException, TimeoutException {
    connection.setHecEndpointType(Connection.HecEndpoint.RAW_EVENTS_ENDPOINT);
    connection.setTestName(JSON_TO_RAW_WITH_BUFFERING);
    super.eventType = EventType.JSON;
    configureConnectionForMetrics(connection);
    sendWithMetrics();
  }

  @Test
  public void sendTextToEventsEndpointWithBuffering() throws InterruptedException, TimeoutException {
    connection.setHecEndpointType(Connection.HecEndpoint.STRUCTURED_EVENTS_ENDPOINT);
    connection.setTestName(TEXT_TO_EVENTS_WITH_BUFFERING);
    super.eventType = EventType.TEXT;
    configureConnectionForMetrics(connection);
    sendWithMetrics();
  }  

    @Test
  public void sendJsonToEventsEndpointWithBuffering() throws InterruptedException, TimeoutException {
    connection.setHecEndpointType(Connection.HecEndpoint.STRUCTURED_EVENTS_ENDPOINT);
    connection.setTestName(JSON_TO_EVENTS_WITH_BUFFERING);
    super.eventType = EventType.JSON;
    configureConnectionForMetrics(connection);
    sendWithMetrics();
  }

  // TODO: review what fields to include in summary and ack logs
  private void logResults(long start, long end) {
    /*
      Description of data:

      ** The below is logged when an event batch is acknowledged **

      test_id: unique ID for each @test
      run_id: unique ID that is the same across each test in the class
      test_name: name of the test
      endpoint: raw vs. event
      mock: true if using mock HEC endpoint (from lb.properties)
      splunk_type: description of deployment destination
      buffer_size: size of internal buffer used by cloudfwd
      url_list: all of the urls that this connection is sending to
      channels_per_destination: # channels per IP address destination (from lb.properties)
      label: a key for locating these logging lines if grepping

     */
    System.out.println(
            "test_id=" + connection.getTestId() +
            " run_id=" + run_id +
            " test_name=" + connection.getTestName() +
            " endpoint=" + connection.getHecEndpointType() +
            " mock=" + connection.getPropertiesFileHelper().isMockHttp() +
            " splunk_type=" + splunkType +
            " buffer_size=" + connection.getCharBufferSize() +
            " url_list=" + getURLs(connection) +
            " channels_per_destination=" + connection.getPropertiesFileHelper().getChannelsPerDestination() +
            " start_time=" + start +
            " end_time=" + end +
            " duration_seconds=" + (end - start)/1000 +
            " notes=" + notes +
            " label=SUMMARY"
    );
  }

  @Override
  protected Properties getProps() {
    Properties props = new Properties();
    props.put(PropertiesFileHelper.MOCK_HTTP_KEY, "false");
    return props;
  }


  @Override
  protected int getNumEventsToSend() {
    return numToSend;
  }

  private void sendWithMetrics() throws TimeoutException, InterruptedException {
    long start = System.currentTimeMillis();
    super.sendEvents();
    long end = System.currentTimeMillis();
    logResults(start, end);
  }

  private void configureConnectionForMetrics(Connection connection) {
    connection.setCharBufferSize(bufferSize);
    connection.setRunId(run_id);
    connection.setTestId(UUID.randomUUID().toString());
  }

  private String getURLs(Connection c) {
    List<URL> urls = c.getPropertiesFileHelper().getUrls();
    StringBuilder urlList = new StringBuilder();
    for (URL url : urls) {
      urlList.append(url.toString()).append(", ");
    }
    return urlList.toString();
  }

}
