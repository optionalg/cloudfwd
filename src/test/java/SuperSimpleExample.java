
import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.EventBatch;
import com.splunk.cloudfwd.util.PropertiesFileHelper;
import com.splunk.cloudfwd.RawEvent;
import java.util.Properties;
import com.splunk.cloudfwd.ConnectonCallbacks;
import com.splunk.cloudfwd.EventWithMetadata;
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

  public static void main(String[] args) {
    final int numEvents = 10000;

    ConnectonCallbacks callbacks = new ConnectonCallbacks() {
      @Override
      public void acknowledged(EventBatch events) {
        System.out.println(
                "EventBatch  " + events.getId() + " has been index-acknowledged by Splunk.");
      }

      @Override
      public void failed(EventBatch events, Exception ex) {
        if (events != null) {
          System.out.println("EventBatch " + events.getId() + " failed: " + ex.
                  getMessage());
        } else {
          System.out.println("An exception occurred: " + ex.getMessage());
        }
      }

      @Override
      public void checkpoint(EventBatch events) {
       // if (events.getId().compareTo(new Integer(numEvents)) == 0) {
          System.out.println(
                  "CHECKPOINT: " + events.getId() + " (all events up to and including this ID are acknowledged)");
        //}

      }
    }; //end callbacks

    //overide defaults in lb.properties
    Properties customization = new Properties();
    customization.put(PropertiesFileHelper.COLLECTOR_URI,
            "https://127.0.0.1:8088");
    customization.put(PropertiesFileHelper.TOKEN_KEY,
            "ad9017fd-4adb-4545-9f7a-62a8d28ba7b3");
    customization.put(PropertiesFileHelper.UNRESPONSIVE_MS, "100000");//100 sec - Kill unresponsive channel
    customization.put(PropertiesFileHelper.MOCK_HTTP_KEY, "true");
    customization.put(PropertiesFileHelper.MAX_TOTAL_CHANNELS, "1"); //increase this to increase parallelism 

    //date formatter for sending 'raw' event 
    SimpleDateFormat dateFormat = new SimpleDateFormat(
            "yyyy-MM-dd HH:mm:ss");//one of many supported Splunk timestamp formats

    //SEND TEXT EVENTS TO HEC 'RAW' ENDPOINT  
    try (Connection c = new Connection(callbacks, customization);) {
      c.setCharBufferSize(1024 * 16); //16kB send buffering -- in practice use a much larger buffer
      c.setSendTimeout(10000); //10 sec
      for (int seqno = 1; seqno <= numEvents; seqno++) {//sequence numbers can be any Comparable Object
        //generate a 'raw' text event looking like "2017-08-10 11:21:04 foo bar baz"
        String eventData = dateFormat.format(new Date()) + " foo bar baz";
        RawEvent event = RawEvent.fromText(eventData, seqno);
        c.send(event);
      }
    } //safely autocloses Connection, no event loss. (use Connection.closeNow() if you want to *lose* in-flight events)

    //SEND STRUCTURED EVENTS TO HEC 'EVENT' ENDPOINT
    try (Connection c = new Connection(callbacks, customization);) {
      c.setCharBufferSize(1024 * 16); //16kB send buffering
      c.setSendTimeout(10000); //10 sec        
      c.setHecEndpointType(Connection.HecEndpoint.STRUCTURED_EVENTS_ENDPOINT);
      for (int seqno = 1; seqno <= numEvents; seqno++) {
        EventWithMetadata event = new EventWithMetadata(getStructuredEvent(),
                seqno);
        c.send(event);
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
