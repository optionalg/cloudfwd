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
import java.util.concurrent.TimeoutException;

import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

/**
 *
 * @author ajayaraman
 */
public class HecEndpointEventTypeTests extends AbstractConnectionTest {

  //private static Map<String, String> message;
  private EventBatch.Endpoint endpoint;
  private EventBatch.Eventtype eventType;


  @Override
  protected int getNumBatchesToSend() {
    return 1;
  }

  @Override
  protected EventBatch nextEventBatch() {
    EventBatch events = new EventBatch(this.endpoint, this.eventType);
    events.add(new HttpEventCollectorEvent("info", "foo", "HEC_LOGGER",
            Thread.currentThread().getName(), new HashMap(), null, null));
    return events;
  }

  @Override
  protected Properties getProps() {
    Properties props = new Properties();
    props.put(PropertiesFileHelper.MOCK_HTTP_KEY, "false");
    return props;
  }

  @Test
  public void testBlobToRaw() throws Exception {
    System.out.println("testBlobToRaw");
    this.endpoint = EventBatch.Endpoint.raw;
    this.eventType = EventBatch.Eventtype.blob;
    super.sendEvents();
  }

  @Test
  public void testJsonToRaw() throws Exception {
    System.out.println("testJsonToRaw");
    this.endpoint = EventBatch.Endpoint.raw;
    this.eventType = EventBatch.Eventtype.json;
    super.sendEvents();

  }

  @Test
  public void testBlobToEvent() throws Exception {
    System.out.println("testBlobToEvent");
    this.endpoint = EventBatch.Endpoint.event;
    this.eventType = EventBatch.Eventtype.blob;
    super.sendEvents();

  }

  @Test
  public void testJsonToEvent() throws Exception {
    System.out.println("testJsonToEvent");
    this.endpoint = EventBatch.Endpoint.event;
    this.eventType = EventBatch.Eventtype.json;
    super.sendEvents();

  }
  
  public static void main(String[] args){
    new HecEndpointEventTypeTests().runTests();    
  }
}
