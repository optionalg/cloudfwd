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

import com.splunk.cloudfwd.PropertiesFileHelper;
import com.splunk.cloudfwd.EventBatch;
import com.splunk.cloudfwd.http.HttpEventCollectorEvent;

import java.util.*;
import org.junit.Test;

/**
 *
 * @author ajayaraman
 */
public class HecEndpointEventTypeTest extends AbstractConnectionTest {

  private EventBatch.Endpoint endpoint;
  private String eventtype;
  private String eventtype2 = null;
  private Map<String, String> message = new HashMap<String, String>() {
  {
    put("json", "{\"event\":\"Json\"}");
    put("blob", "event: Blob");
  }};

  @Override
  protected int getNumBatchesToSend() {
    return 1;
  }

  @Override
  protected EventBatch nextEventBatch() {
    this.eventtype2 = ((this.eventtype2 == null ? this.eventtype : this.eventtype2));
    EventBatch events = new EventBatch(this.endpoint);
    events.add(new HttpEventCollectorEvent("info",
            message.get(this.eventtype),
            "HEC_LOGGER",
            Thread.currentThread().getName(),
            new HashMap(),
            null,
            null));
        events.add(new HttpEventCollectorEvent("info",
            message.get(this.eventtype2),
            "HEC_LOGGER",
            Thread.currentThread().getName(),
            new HashMap(),
            null,
            null));

    return events;
  }

  @Override
  protected Properties getProps() {
    Properties props = new Properties();
    props.put(PropertiesFileHelper.MOCK_HTTP_KEY, "true");
    return props;
  }

  @Test
  public void testBlobToRaw() throws Exception {
    System.out.println("testBlobToRaw");
    this.eventtype = "blob";
    this.endpoint = EventBatch.Endpoint.RAW;
    super.sendEvents();
  }

  @Test
  public void testJsonToRaw() throws Exception {
    System.out.println("testJsonToRaw");
    this.eventtype = "json";
    this.endpoint = EventBatch.Endpoint.RAW;
    super.sendEvents();
  }

  @Test
  public void testBlobToEvent() throws Exception {
    System.out.println("testBlobToEvent");
    this.eventtype = "blob";
    this.endpoint = EventBatch.Endpoint.EVENT;
    super.sendEvents();
  }

  @Test
  public void testJsonToEvent() throws Exception {
    System.out.println("testJsonToEvent");
    this.eventtype = "json";
    this.endpoint = EventBatch.Endpoint.EVENT;
    super.sendEvents();
  }

  @Test(expected = IllegalStateException.class)
  public void testThrowsJsonBlobToRaw() throws Exception {
    System.out.println("testThrowsJsonBlobToRaw");
    this.eventtype = "json";
    this.eventtype2 = "blob";
    this.endpoint = EventBatch.Endpoint.RAW;
    super.sendEvents();
  }

  public static void main(String[] args){
    new HecEndpointEventTypeTest().runTests();
  }
}
