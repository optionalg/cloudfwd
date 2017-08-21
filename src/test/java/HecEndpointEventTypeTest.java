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
import com.splunk.cloudfwd.RawEvent;
import java.io.IOException;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.Test;

/**
 *
 * @author ajayaraman
 */
public class HecEndpointEventTypeTest extends AbstractConnectionTest {

  protected enum EventType {
    TEXT, JSON
  }
  protected EventType eventType;

  @Override
  protected int getNumEventsToSend() {
    return 1;
  }

  @Override
  protected Event nextEvent(int seqno) {
    switch (this.eventType) {
      case TEXT: {
        if (connection.getHecEndpointType() == HecEndpoint.RAW_EVENTS_ENDPOINT) {
          return RawEvent.fromText("nothing to see here.", seqno);
        } else {
          return new EventWithMetadata("nothing to see here.", seqno);
        }
      }
      case JSON: {
        if (connection.getHecEndpointType() == HecEndpoint.RAW_EVENTS_ENDPOINT) {
          try {
            return RawEvent.fromObject(getStructuredEvent(), seqno);
          } catch (IOException ex) {
            Logger.getLogger(HecEndpointEventTypeTest.class.getName()).
                    log(Level.SEVERE, null, ex);
            throw new RuntimeException(ex.getMessage(), ex);
          }
        } else {
          return new EventWithMetadata(getStructuredEvent(), seqno);
        }
      }
    }
    throw new RuntimeException("Unknown event type in test");
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
    this.eventType=EventType.TEXT;
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

  private Object getStructuredEvent() {
    Map map = new LinkedHashMap();
    map.put("foo", "bar");
    map.put("baz", "nothing to see here");
    return map;
  }

}
