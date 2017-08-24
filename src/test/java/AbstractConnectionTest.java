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

  //used by tests that need to set eventType
  protected enum EventType {
    TEXT, JSON
  }
  protected EventType eventType = EventType.TEXT; //default to TEXT event content

  @Before
  public void setUp() {
    this.callbacks = getCallbacks();
    this.connection = new Connection((ConnectionCallbacks) callbacks, getProps());

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
    int expected = getNumEventsToSend();
    for (int i = 0; i < expected; i++) {
      ///final EventBatch events =nextEventBatch(i+1);
      Event event = nextEvent(i + 1);
      System.out.println("Send event: " + event.getId() + " i=" + i);
      this.connection.send(event); //will immediately send event in batch since buffer defaults to zero
    }
    connection.close(); //will flush
    this.callbacks.await(10, TimeUnit.MINUTES);
    if(callbacks.isFailed()){
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
    switch (this.eventType) {
      case TEXT: {
        if (connection.getHecEndpointType() == Connection.HecEndpoint.RAW_EVENTS_ENDPOINT) {
          return getTimestampedRawEvent(seqno);
        } else {
          return new EventWithMetadata("nothing to see here.", seqno);
        }
      }
      case JSON: {
        if (connection.getHecEndpointType() == Connection.HecEndpoint.RAW_EVENTS_ENDPOINT) {
          try {
            return RawEvent.fromObject(getStructuredEvent(), seqno);
          } catch (IOException ex) {
            Logger.getLogger(AbstractConnectionTest.class.getName()).
                    log(Level.SEVERE, null, ex);
            throw new RuntimeException(ex.getMessage(),ex);
          }
        } else {
          return new EventWithMetadata(getStructuredEvent(), seqno);
        }
      }
    }
    throw new RuntimeException("Unknown event type in test");
  }

  protected Object getStructuredEvent() {
    Map map = new LinkedHashMap();
    map.put("foo", "bar");
    map.put("baz", "nothing to see here");
    return map;
  }

  protected abstract int getNumEventsToSend();

  protected BasicCallbacks getCallbacks() {
    return new BasicCallbacks(getNumEventsToSend());
  }

  protected RawEvent getTimestampedRawEvent(int seqno) {
    return RawEvent.fromText(
            dateFormat.format(new Date()) + " nothing to see here", seqno);
  }

}
