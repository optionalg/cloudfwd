import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.Event;
import com.splunk.cloudfwd.EventWithMetadata;
import com.splunk.cloudfwd.RawEvent;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import com.splunk.cloudfwd.ConnectionCallbacks;

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
/**
 *
 * @author ghendrey
 */
public abstract class AbstractConnectionTest {

  protected BasicCallbacks callbacks;
  protected Connection connection;
  protected SimpleDateFormat dateFormat = new SimpleDateFormat(
          "yyyy-MM-dd HH:mm:ss");
  protected final String TEST_CLASS_INSTANCE_GUID = java.util.UUID.randomUUID().
          toString();
  protected String testMethodGUID;
  protected String testMethodGUIDKey = "testMethodGUID";
  protected List<Event> events;

  
  //override to do stuff like set buffering or anything else affecting connection
  protected void configureConnection(Connection connection) {
    //noop
  }

  //used by tests that need to set eventType
  protected enum EventType {
    TEXT, JSON
  }
  protected EventType eventType = EventType.TEXT; //default to TEXT event content

  @Before
  public void setUp() {
    this.callbacks = getCallbacks();
    this.connection = new Connection((ConnectionCallbacks) callbacks, getProps());
    configureConnection(connection);
    this.testMethodGUID = java.util.UUID.randomUUID().toString();
    this.events = new ArrayList<>();

  }

  @After
  public void tearDown() {
    //in case of failure we probably have events stuck on a channel. Therefore a regular close will just
    //hang out waiting (infinitely?) for the messages to flush out before gracefully closing. So when we see
    //a failure we must use the closeNow method which closes the channel regardless of whether it has
    //messages in flight.
    if (callbacks.isFailed()) {
      connection.closeNow();
    }
  }

  protected void sendEvents() throws TimeoutException, InterruptedException {
    System.out.println(
            "SENDING EVENTS WITH CLASS GUID: " + TEST_CLASS_INSTANCE_GUID +
                    "And test method GUID " + testMethodGUID);
    int expected = getNumEventsToSend();
    for (int i = 0; i < expected; i++) {
      ///final EventBatch events =nextEventBatch(i+1);
      Event event = nextEvent(i + 1);
      System.out.println("Send event: " + event.getId() + " i=" + i);
      this.connection.send(event); //will immediately send event in batch since buffer defaults to zero
    }
    connection.close(); //will flush
    this.callbacks.await(10, TimeUnit.MINUTES);
    if (callbacks.isFailed()) {
      Assert.fail(callbacks.getFailMsg());
    }
  }

  protected abstract Properties getProps();

  /**
   * Default implementation will return the
   * @param seqno
   * @return
   */
  private Event nextEvent(int seqno) {
    Event event;
    switch (this.eventType) {
      case TEXT: {
        if (connection.getHecEndpointType() == Connection.HecEndpoint.RAW_EVENTS_ENDPOINT) {
          event = getTimestampedRawEvent(seqno);
          if (shouldCacheEvents()) events.add(event);
          return event;
        } else {
          event = new EventWithMetadata("TEXT FOR /events endpoint 'event' field with "
                          + getEventTracingInfo() + " seqno=" + seqno,
                  seqno);
          if (shouldCacheEvents()) events.add(event);
          return event;
        }
      }
      case JSON: {
        if (connection.getHecEndpointType() == Connection.HecEndpoint.RAW_EVENTS_ENDPOINT) {
          try {
            Map m = getStructuredEvent();
            m.put("where_to", "/raw");
            m.put("seqno", Integer.toString(seqno));
            event = RawEvent.fromObject(m, seqno);
            if (shouldCacheEvents()) events.add(event);
            return event;
          } catch (IOException ex) {
            Logger.getLogger(AbstractConnectionTest.class.getName()).
                    log(Level.SEVERE, null, ex);
            throw new RuntimeException(ex.getMessage(), ex);
          }
        } else {
          Map m = getStructuredEvent();
          m.put("where_to", "/events");
          m.put("seqno", Integer.toString(seqno));
          event = new EventWithMetadata(m, seqno);
          if (shouldCacheEvents()) events.add(event);
          return event;
        }
      }
    }
    throw new RuntimeException("Unknown event type in test");
  }

  protected Map getStructuredEvent() {
    Map map = new LinkedHashMap();
    map.put("foo", "bar");
    map.put("baz", "yeah I am json field");
    map.put("trace", getEventTracingInfo());
    map.put(testMethodGUIDKey, testMethodGUID);
    return map;
  }

  protected abstract int getNumEventsToSend();

  protected BasicCallbacks getCallbacks() {
    return new BasicCallbacks(getNumEventsToSend());
  }

  protected RawEvent getTimestampedRawEvent(int seqno) {
    return RawEvent.fromText(//dateFormat.format(new Date()) + " TEXT FOR /raw ENDPOINT", seqno);
            "TEXT FOR /raw ENDPOINT with " + getEventTracingInfo() + " seqno=" + seqno,
            seqno);
  }

  protected String getEventTracingInfo() {
    return "GUID=" + TEST_CLASS_INSTANCE_GUID + " " + testMethodGUIDKey + "=" + testMethodGUID;
  }

  // override if you need to access the events you send
  protected boolean shouldCacheEvents() {
    return false;
  }

  protected List<Event> getSentEvents() {
    if (!shouldCacheEvents()) {
      throw new RuntimeException("Events were not cached. Override shouldCacheEvents() to store sent events.");
    }
    return events;
  }

}
