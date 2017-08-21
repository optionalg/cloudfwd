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

import com.splunk.cloudfwd.Connection.HecEndpoint;
import com.splunk.cloudfwd.PropertiesFileHelper;
import com.splunk.cloudfwd.Event;
import com.splunk.cloudfwd.EventWithMetadata;

import java.util.*;
import org.junit.Test;

/**
 *
 * @author ajayaraman
 */
public class HecEndpointEventTypeTest extends AbstractConnectionTest {

  @Override
  protected int getNumEventsToSend() {
    return 1;
  }

 
  @Override
  protected Properties getProps() {
    Properties props = new Properties();
    props.put(PropertiesFileHelper.MOCK_HTTP_KEY, "false");
    return props;
  }

  @Test
  public void testTextToRaw() throws Exception {
    System.out.println("testBlobToRaw");
    this.eventType = EventType.TEXT;
    setEndpointType(HecEndpoint.RAW_EVENTS_ENDPOINT);
    super.sendEvents();
  }

  @Test
  public void testJsonToRaw() throws Exception {
    System.out.println("testJsonToRaw");
    this.eventType = EventType.JSON;
    setEndpointType(HecEndpoint.RAW_EVENTS_ENDPOINT);
    super.sendEvents();

  }

  @Test
  public void testTextToEvent() throws Exception {
    System.out.println("testBlobToEvent");
    this.eventType = EventType.TEXT;
    setEndpointType(HecEndpoint.STRUCTURED_EVENTS_ENDPOINT);
    super.sendEvents();

  }

  @Test
  public void testJsonToEvent() throws Exception {
    System.out.println("testJsonToEvent");
    this.eventType = EventType.JSON;
    setEndpointType(HecEndpoint.STRUCTURED_EVENTS_ENDPOINT);
    super.sendEvents();

  }

  private void setEndpointType(HecEndpoint type) {
    super.connection.setHecEndpointType(type);
  }

}
