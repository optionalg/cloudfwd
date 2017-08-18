
import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.Event;
import com.splunk.cloudfwd.EventBatch;
import com.splunk.cloudfwd.FutureCallback;
import com.splunk.cloudfwd.PropertiesFileHelper;
import java.util.Properties;

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
    FutureCallback callback = new FutureCallback() {
      @Override
      public void acknowledged(EventBatch events) {
        System.out.println(
                "EventBatch " + events.getId() + " was indexed by Splunk.");
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
        System.out.println("CHECKPOINT: " + events.getId());
      }
    }; //end callback
    
    //overide defaults in lb.properties
    Properties customization = new Properties();
    customization.put(PropertiesFileHelper.COLLECTOR_URI, "https://127.0.0.1:8088");
    customization.put(PropertiesFileHelper.TOKEN_KEY, "dab493e1-26aa-4916-9570-c7a169a2e433");
    customization.put(PropertiesFileHelper.UNRESPONSIVE_MS, "5000");
    //use a simulated Splunk HEC
    customization.put(PropertiesFileHelper.MOCK_HTTP_KEY, "true");
    
    Connection c = new Connection(callback, customization);
    for(long i=0;i<1000;i++){
      EventBatch events = new EventBatch(EventBatch.Endpoint.event, EventBatch.Eventtype.blob);
      events.setSeqNo(i);
      for(int j=0;j<10;j++){
        Event e = new Event
      }
    }
    
  
  }

}
