package mock_tests;/*
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
import com.splunk.cloudfwd.ConnectionSettings;
import com.splunk.cloudfwd.Event;
import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.impl.util.PropertiesFileHelper;
import test_utils.AbstractConnectionTest;
import java.util.Properties;
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
    protected void setProps(PropertiesFileHelper settings) {
      settings.setAckTimeoutMS(1000000);
      settings.setUnresponsiveMS(-1); //no dead channel detection
  }

  @Override
  protected void configureConnection(Connection connection) {
    connection.getSettings().setEventBatchSize(1024*32); //32k batching batching, roughly
  }


  @Test
  public void sendTextToRawEndpointWithBuffering() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
    connection.getSettings().setHecEndpointType(Connection.HecEndpoint.RAW_EVENTS_ENDPOINT);
    super.eventType = Event.Type.TEXT;
    super.sendEvents();
  }

    @Test
  public void sendJsonToRawEndpointWithBuffering() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
    connection.getSettings().setHecEndpointType(Connection.HecEndpoint.RAW_EVENTS_ENDPOINT);
    super.eventType = Event.Type.JSON;
    super.sendEvents();
  }


  @Test
  public void sendTextToEventsEndpointWithBuffering() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
    connection.getSettings().setHecEndpointType(Connection.HecEndpoint.STRUCTURED_EVENTS_ENDPOINT);
    super.eventType = Event.Type.TEXT;
    super.sendEvents();
  }

    @Test
  public void sendJsonToEventsEndpointWithBuffering() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
    connection.getSettings().setHecEndpointType(Connection.HecEndpoint.STRUCTURED_EVENTS_ENDPOINT);
    super.eventType = Event.Type.JSON;
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
