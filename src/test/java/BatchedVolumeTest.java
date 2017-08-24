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
  private String CLUSTER_LOCAL = "CLUSTER_LOCAL";
  private String CLUSTER_CLOUD = "CLUSTER_CLOUD";

  private String run_id = UUID.randomUUID().toString(); // All 4 tests in this suite will have the same run ID

  /* ************************************* SETTINGS **************************************** */
  private String splunkType = CLUSTER_LOCAL; // just a label - change this before every test run
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

  private void logResults(long start, long end) {
    System.out.println(
            "test_id=" + connection.getTestId() +
            " run_id=" + run_id +
            " test_name=" + connection.getTestName() +
            " start_time=" + start +
            " end_time=" + end +
            " elapsed_time_seconds=" + (end - start)/1000 +
            " splunk_type=" + splunkType +
            " label=PERF"
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

}
