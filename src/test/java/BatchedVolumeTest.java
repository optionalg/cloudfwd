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
import com.splunk.cloudfwd.HecConnectionTimeoutException;
import com.splunk.cloudfwd.IConnection;
import org.junit.Test;
import java.util.concurrent.TimeoutException;

/**
 *
 * @author ghendrey
 */
public class BatchedVolumeTest extends AbstractConnectionTest {

  protected int numToSend = 1000000;

  public BatchedVolumeTest() {
  }

  @Override
  protected void configureConnection(IConnection connection) {
    connection.setEventBatchSize(1024*32); //32k batching batching, roughly
  }



  @Test
  public void sendTextToRawEndpointWithBuffering() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
    connection.setHecEndpointType(Connection.HecEndpoint.RAW_EVENTS_ENDPOINT);
    super.eventType = EventType.TEXT;
    super.sendEvents();
  }


    @Test
  public void sendJsonToRawEndpointWithBuffering() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
    connection.setHecEndpointType(Connection.HecEndpoint.RAW_EVENTS_ENDPOINT);
    super.eventType = EventType.JSON;
    super.sendEvents();
  }



  @Test
  public void sendTextToEventsEndpointWithBuffering() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
    connection.setHecEndpointType(Connection.HecEndpoint.STRUCTURED_EVENTS_ENDPOINT);
    super.eventType = EventType.TEXT;
    super.sendEvents();
  }

    @Test
  public void sendJsonToEventsEndpointWithBuffering() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
    connection.setHecEndpointType(Connection.HecEndpoint.STRUCTURED_EVENTS_ENDPOINT);
    super.eventType = EventType.JSON;
    super.sendEvents();
  }


/*
  @Override
  protected Properties getProps() {
    Properties props = new Properties();
    props.put(PropertiesFileHelper.MOCK_HTTP_KEY, "true");
    return props;
  }
*/

  @Override
  protected int getNumEventsToSend() {
    return numToSend;
  }

}
