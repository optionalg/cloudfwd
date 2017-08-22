
import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.EventBatch;
import com.splunk.cloudfwd.util.PropertiesFileHelper;
import com.splunk.cloudfwd.RawEvent;
import java.util.Properties;
import com.splunk.cloudfwd.ConnectonCallbacks;

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
    ConnectonCallbacks callback = new ConnectonCallbacks() {
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
        System.out.println("CHECKPOINT: " + events.getId() + " (all events up to and including this ID are acknowledged)");
      }
    }; //end callback

    //overide defaults in lb.properties
    Properties customization = new Properties();
    customization.put(PropertiesFileHelper.COLLECTOR_URI,
            "https://127.0.0.1:8088");
    customization.put(PropertiesFileHelper.TOKEN_KEY,
            "dab493e1-26aa-4916-9570-c7a169a2e433");
    customization.put(PropertiesFileHelper.UNRESPONSIVE_MS, "5000");
    //use a simulated Splunk HEC
    customization.put(PropertiesFileHelper.MOCK_HTTP_KEY, "true");

    try (Connection c = new Connection(callback, customization);) {
      c.setCharBufferSize(1024 * 16); //16kB send buffering
      for (long i = 0; i < 100000; i++) {
        c.send(RawEvent.fromText("nothing to see here.", i));
      }
    }//autoclose will flush buffers and deliver events

  }

}
