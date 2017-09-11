
import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.Event;
import com.splunk.cloudfwd.EventWithMetadata;
import com.splunk.cloudfwd.RawEvent;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import com.splunk.cloudfwd.ConnectionCallbacks;
import com.splunk.cloudfwd.HecConnectionTimeoutException;
import com.splunk.cloudfwd.UnvalidatedByteBufferEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.InputStream;
import java.nio.ByteBuffer;

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

  private static final Logger LOG = LoggerFactory.getLogger(AbstractConnectionTest.class.getName());

  /**
   * set enabled=false in test.properties to disable test.properties and
   * fallback on lb.properties
   */
  public static final String KEY_ENABLE_TEST_PROPERTIES = "enabled";
  public static final String TEST_METHOD_GUID_KEY = "testMethodGUID";

  protected BasicCallbacks callbacks;
  protected Connection connection;
  protected SimpleDateFormat dateFormat = new SimpleDateFormat(
          "yyyy-MM-dd HH:mm:ss");
  protected final static String TEST_CLASS_INSTANCE_GUID = java.util.UUID.
          randomUUID().
          toString();
  protected String testMethodGUID;
  protected List<Event> events;

  //override to do stuff like set buffering or anything else affecting connection
  protected void configureConnection(Connection connection) {
    //noop
  }

  protected Event.Type eventType = Event.Type.TEXT; //default to TEXT event content

  @Before
  public void setUp() {
    this.callbacks = getCallbacks();
    Properties props = new Properties();
    props.putAll(getTestProps());
    props.putAll(getProps());
    this.connection = new Connection((ConnectionCallbacks) callbacks, props);
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

  protected void sendEvents() throws InterruptedException, HecConnectionTimeoutException {
    LOG.trace(
            "SENDING EVENTS WITH CLASS GUID: " + TEST_CLASS_INSTANCE_GUID
            + "And test method GUID " + testMethodGUID);
    int expected = getNumEventsToSend();
    for (int i = 0; i < expected; i++) {
      ///final EventBatch events =nextEventBatch(i+1);
      Event event = nextEvent(i + 1);
      LOG.trace("Send event: " + event.getId() + " i=" + i);
      connection.send(event);
//      if (i == 300000) connection.setUrls("https://127.0.0.1:8288");
//      if (i == 600000) connection.setUrls("https://127.0.0.1:8388");
    }
    connection.close(); //will flush
    this.callbacks.await(10, TimeUnit.MINUTES);
    if (callbacks.isFailed()) {
      Assert.fail(
              "There was a failure callback with exception class  " + callbacks.
              getException() + " and message " + callbacks.getFailMsg());
    }
  }

  protected void sendCombinationEvents() throws TimeoutException, InterruptedException, HecConnectionTimeoutException {
    LOG.trace("SENDING EVENTS WITH CLASS GUID: " + TEST_CLASS_INSTANCE_GUID
            + "And test method GUID " + testMethodGUID);
    int expected = getNumEventsToSend();
    for (int i = 0; i < expected; i++) {
      ///final EventBatch events =nextEventBatch(i+1);
      if (i % 2 == 1) {
        this.eventType = Event.Type.TEXT;
      } else {
        this.eventType = Event.Type.JSON;
      }
      Event event = nextEvent(i + 1);
      LOG.trace("Send event: " + event.getId() + " i=" + i);
      this.connection.send(event); //will immediately send event in batch since buffer defaults to zero
    }
    connection.close(); //will flush
    this.callbacks.await(10, TimeUnit.MINUTES);
    if (callbacks.isFailed()) {
      Assert.fail(callbacks.getFailMsg());
    }
  }

  /**
   * test should override this to add properties on top of
   * lb.properties+test.properties
   *
   * @return
   */
  protected Properties getProps() {
    return new Properties(); //default behavior is no "hard coded" test-specific properties
  }

  /**
   * reads default test properties out of test_defaults.properties (these
   * overlay on top of lb.properties)
   *
   * @return
   */
  protected Properties getTestProps() {
    Properties props = new Properties();
    try (InputStream is = getClass().getResourceAsStream(
            getTestPropertiesFileName());) {
      if (null != is) {
        props.load(is);
      } else {
        LOG.trace("No test_defaults.properties found on classpath");
      }
    } catch (IOException ex) {
      LOG.error(ex.getMessage(), ex);
    }
    if (Boolean.parseBoolean(props.getProperty("enabled", "false"))) {
      return props;
    } else {
      LOG.warn("test.properties disabled, using lb.properties only");
      return new Properties(); //ignore test.properties
    }
  }

  /**
   * test can override this if a test requires its own .properties file to slap
   * on top of lb.properties (instead of slapping test.properties on top of
   * lb.properties)
   *
   * @return
   */
  protected String getTestPropertiesFileName() {
    return "test.properties";
  }

  /**
   * Default implementation will return the next event to send
   *
   * @param seqno
   * @return
   */
  protected Event nextEvent(int seqno) {
    Event event = null;
    switch (this.eventType) {
      case TEXT: {
        event = getTextEvent(seqno);
        break;
      }
      case JSON: {
        event = getJsonEvent(seqno);
        break;
      }
      case UNKNOWN: {
        event = getUnvalidatedBytesEvent(seqno);
        break;
      }
      default: {
        throw new RuntimeException("unsupported type");
      }
    }
    if (shouldCacheEvents()) {
      events.add(event);
    }
    return event;
  }

  private Event getUnvalidatedBytesEvent(int seqno) {
    Event event;
    if (connection.getHecEndpointType() == Connection.HecEndpoint.RAW_EVENTS_ENDPOINT) {
      event = getUnvalidatedBytesToRawEndpoint(seqno);
    } else {
      event = getUnvalidatedBytesToEventEndpoint(seqno);
    }
    return event;
  }

  protected Event getJsonEvent(int seqno) {
    Event event;
    if (connection.getHecEndpointType() == Connection.HecEndpoint.RAW_EVENTS_ENDPOINT) {
      event = getJsonToRawEndpoint(seqno);
    } else {
      event = getJsonToEvents(seqno);
    }
    return event;
  }

  protected Event getTextEvent(int seqno) {
    Event event;
    if (connection.getHecEndpointType() == Connection.HecEndpoint.RAW_EVENTS_ENDPOINT) {
      event = getTimestampedRawEvent(seqno);
    } else {
      event = getTextToEvents(seqno);
    }
    return event;
  }

  protected Map getStructuredEvent() {
    Map map = new LinkedHashMap();
    map.put("foo", "bar");
    map.put("baz", "yeah I am json field");
    map.put("trace", getEventTracingInfo());
    map.put(TEST_METHOD_GUID_KEY, testMethodGUID);
    return map;
  }

  protected abstract int getNumEventsToSend();

  protected BasicCallbacks getCallbacks() {
    return new BasicCallbacks(getNumEventsToSend());
  }

  protected RawEvent getTimestampedRawEvent(int seqno) {
    return RawEvent.
            fromText(//dateFormat.format(new Date()) + " TEXT FOR /raw ENDPOINT", seqno);
                    "TEXT FOR /raw ENDPOINT with " + getEventTracingInfo() + " seqno=" + seqno,
                    seqno);
  }

  protected String getEventTracingInfo() {
    return "GUID=" + TEST_CLASS_INSTANCE_GUID + " " + TEST_METHOD_GUID_KEY + "=" + testMethodGUID;
  }

  // override if you need to access the events you send
  protected boolean shouldCacheEvents() {
    return false;
  }

  protected List<Event> getSentEvents() {
    if (!shouldCacheEvents()) {
      throw new RuntimeException(
              "Events were not cached. Override shouldCacheEvents() to store sent events.");
    }
    return events;
  }

  protected Event getJsonToRawEndpoint(int seqno) {
    try {
      Map m = getStructuredEvent();
      m.put("where_to", "/raw");
      m.put("seqno", Integer.toString(seqno));
      Event event = RawEvent.fromObject(m, seqno);
      return event;
    } catch (IOException ex) {
      LOG.error(ex.getMessage(), ex);
      throw new RuntimeException(ex.getMessage(), ex);
    }
  }

  protected Event getJsonToEvents(int seqno) {
    Map m = getStructuredEvent();
    m.put("where_to", "/events");
    m.put("seqno", Integer.toString(seqno));
    Event event = new EventWithMetadata(m, seqno);
    return event;
  }

  protected Event getTextToEvents(int seqno) {
    Event event = new EventWithMetadata(
            "TEXT FOR /events endpoint 'event' field with "
            + getEventTracingInfo() + " seqno=" + seqno,
            seqno);
    return event;
  }

  private Event getUnvalidatedBytesToRawEndpoint(int seqno) {
    //create a valid JSON to /events, grab its bytes, and wrap it in UnvalidatedBytes to simulate
    //the creation of /event endpoint envelope "by hand"
//    return new UnvalidatedBytesEvent(getJsonToRawEndpoint(seqno).getBytes(),
//            seqno);
    return new UnvalidatedByteBufferEvent(ByteBuffer.wrap(getJsonToRawEndpoint(seqno).getBytes()), seqno);
  }

  private Event getUnvalidatedBytesToEventEndpoint(int seqno) {
    //create a valid JSON to /events, grab its bytes, and wrap it in UnvalidatedBytes to simulate
    //the creation of /event endpoint envelope "by hand"
   // return new UnvalidatedBytesEvent(getJsonToEvents(seqno).getBytes(), seqno);
    return new UnvalidatedByteBufferEvent(ByteBuffer.wrap(getJsonToEvents(seqno).getBytes()), seqno);
  }

}
