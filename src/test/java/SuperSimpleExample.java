import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.ConnectionCallbacks;
import com.splunk.cloudfwd.Connections;
import com.splunk.cloudfwd.EventBatch;
import com.splunk.cloudfwd.RawEvent;
import java.util.Properties;
import com.splunk.cloudfwd.EventWithMetadata;
import com.splunk.cloudfwd.HecConnectionTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.splunk.cloudfwd.PropertyKeys.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

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
public class SuperSimpleExample {
  private static final Logger LOG = LoggerFactory.getLogger(SuperSimpleExample.class.getName());


  public static void main(String[] args) {
    final int numEvents = 10000;

    ConnectionCallbacks callbacks = new ConnectionCallbacks() {
      @Override
      public void acknowledged(EventBatch events) {
        LOG.trace("EventBatch  " + events.getId() + " has been index-acknowledged by Splunk.");
      }

      @Override
      public void failed(EventBatch events, Exception ex) {
        if (events != null) {
          LOG.trace("EventBatch " + events.getId() + " failed: " + ex.getMessage());
        } else {
          LOG.trace("An exception occurred: " + ex.getMessage());
        }
      }

      @Override
      public void checkpoint(EventBatch events) {
        // if (events.getId().compareTo(new Integer(numEvents)) == 0) {
        LOG.trace("CHECKPOINT: " + events.getId() + " (all events up to and including this ID are acknowledged)");
        //}
      }

        @Override
        public void systemError(Exception e) {
            LOG.error("Got system error", e);
        }

        @Override
        public void systemWarning(Exception e) {
            LOG.error("Got system warning", e);
        }
    }; //end callbacks

    //overide defaults in lb.properties
    Properties customization = new Properties();
    customization.put(COLLECTOR_URI, "https://127.0.0.1:8088");
    customization.put(TOKEN, "ad9017fd-4adb-4545-9f7a-62a8d28ba7b3");
    customization.put(UNRESPONSIVE_MS, "100000");//100 sec - Kill unresponsive channel
    customization.put(MOCK_HTTP_KEY, "true");
    customization.put(MAX_TOTAL_CHANNELS, "1"); //increase this to increase parallelism

    //date formatter for sending 'raw' event
    SimpleDateFormat dateFormat = new SimpleDateFormat(
            "yyyy-MM-dd HH:mm:ss");//one of many supported Splunk timestamp formats

    //SEND TEXT EVENTS TO HEC 'RAW' ENDPOINT
    try (Connection c = Connections.create(callbacks, customization);) {
      c.getSettings().setEventBatchSize(1024 * 16); //16kB send buffering -- in practice use a much larger buffer
      c.getSettings().setAckTimeoutMS(10000); //10 sec
      for (int seqno = 1; seqno <= numEvents; seqno++) {//sequence numbers can be any Comparable Object
        //generate a 'raw' text event looking like "2017-08-10 11:21:04 foo bar baz"
        String eventData = dateFormat.format(new Date()) + " foo bar baz";
        RawEvent event = RawEvent.fromText(eventData, seqno);
        try {
          c.send(event);
        } catch (HecConnectionTimeoutException ex) {
          //it's up to you to decide how to deal with timeouts. See PropertyKeys.BLOCKING_TIMEOUT_MS
          LOG.error(ex.getMessage(), ex);
        }

      }
    } //safely autocloses Connection, no event loss. (use Connection.closeNow() if you want to *lose* in-flight events)

    //SEND STRUCTURED EVENTS TO HEC 'EVENT' ENDPOINT
    try (Connection c = Connections.create(callbacks, customization);) {
      c.getSettings().setEventBatchSize(1024 * 16); //16kB send buffering
      c.getSettings().setAckTimeoutMS(10000); //10 sec
      c.getSettings().setHecEndpointType(Connection.HecEndpoint.STRUCTURED_EVENTS_ENDPOINT);
      for (int seqno = 1; seqno <= numEvents; seqno++) {
        EventWithMetadata event = new EventWithMetadata(getStructuredEvent(),
                seqno);
        try {
          c.send(event);
        } catch (HecConnectionTimeoutException ex) {
          //it's not a failiure if the connection times out
          LOG.error(ex.getMessage(), ex);
        }
      }
    }

  }

  static Object getStructuredEvent() {
    Map map = new LinkedHashMap();
    map.put("foo", "bar");
    map.put("baz", "nothing to see here");
    return map;
  }

}
