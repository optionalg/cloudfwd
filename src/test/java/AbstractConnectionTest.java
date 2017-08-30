
import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.Event;
import com.splunk.cloudfwd.EventWithMetadata;
import com.splunk.cloudfwd.RawEvent;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import com.splunk.cloudfwd.ConnectionCallbacks;
import com.splunk.cloudfwd.HecConnectionTimeoutException;
import java.io.InputStream;

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

  /**
   * set enabled=false in test.properties to disable test.properties and fallback on lb.properties
   */
  public static final String KEY_ENABLE_TEST_PROPERTIES = "enabled"; 
  protected BasicCallbacks callbacks;
  protected Connection connection;
  protected SimpleDateFormat dateFormat = new SimpleDateFormat(
          "yyyy-MM-dd HH:mm:ss");
  protected final String TEST_CLASS_INSTANCE_GUID = java.util.UUID.randomUUID().
          toString();
  protected String testMethodGUID;

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
    Properties props = new Properties();
    props.putAll(getTestProps());
    props.putAll(getProps());
    this.connection = new Connection((ConnectionCallbacks) callbacks, props);
    configureConnection(connection);
    this.testMethodGUID = java.util.UUID.randomUUID().toString();

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
    System.out.println(
            "SENDING EVENTS WITH CLASS GUID: " + TEST_CLASS_INSTANCE_GUID
            + "And test method GUID " + testMethodGUID);
    int expected = getNumEventsToSend();
    for (int i = 0; i < expected; i++) {
      ///final EventBatch events =nextEventBatch(i+1);
      Event event = nextEvent(i + 1);
      System.out.println("Send event: " + event.getId() + " i=" + i);
      connection.send(event);     
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
        System.out.println("No test_defaults.properties found on classpath");
      }
    } catch (IOException ex) {
      Logger.getLogger(AbstractConnectionTest.class.getName()).
              log(Level.SEVERE, null, ex);
    }
    if (Boolean.parseBoolean(props.getProperty("enabled", "false"))) {
      return props;
    } else {
      Logger.getLogger(AbstractConnectionTest.class.getName()).
              log(Level.WARNING,
                      "test.properties disabled, using lb.properties only");
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
   * Default implementation will return the
   *
   * @param seqno
   * @return
   */
  protected Event nextEvent(int seqno) {
    switch (this.eventType) {
      case TEXT: {
        if (connection.getHecEndpointType() == Connection.HecEndpoint.RAW_EVENTS_ENDPOINT) {
          return getTimestampedRawEvent(seqno);
        } else {
          return new EventWithMetadata(
                  "TEXT FOR /events endpoint 'event' field with " + getEventTracingInfo(),
                  seqno);
        }
      }
      case JSON: {
        if (connection.getHecEndpointType() == Connection.HecEndpoint.RAW_EVENTS_ENDPOINT) {
          try {
            Map m = getStructuredEvent();
            m.put("where_to", "/raw");
            return RawEvent.fromObject(m, seqno);
          } catch (IOException ex) {
            Logger.getLogger(AbstractConnectionTest.class.getName()).
                    log(Level.SEVERE, null, ex);
            throw new RuntimeException(ex.getMessage(), ex);
          }
        } else {
          Map m = getStructuredEvent();
          m.put("where_to", "/events");
          return new EventWithMetadata(m, seqno);
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
    return map;
  }

  protected abstract int getNumEventsToSend();

  protected BasicCallbacks getCallbacks() {
    return new BasicCallbacks(getNumEventsToSend());
  }

  protected RawEvent getTimestampedRawEvent(int seqno) {
    return RawEvent.fromText(//dateFormat.format(new Date()) + " TEXT FOR /raw ENDPOINT", seqno);
            "TEXT FOR /raw ENDPOINT with " + getEventTracingInfo(),
            seqno);
  }

  protected String getEventTracingInfo() {
    return "GUID=" + TEST_CLASS_INSTANCE_GUID + " testMethod GUID= " + testMethodGUID;
  }

}
