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
package com.splunk.cloudfwd.test.perf;

import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.test.integration.AbstractReconciliationTest;
import com.splunk.cloudfwd.test.util.AbstractConnectionTest;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.Test;

/**
 *
 * @author ghendrey
 */
public class ChangeSettingsWhileSendingIT extends AbstractConnectionTest{
    
  protected Properties getProps() {
    Properties p = new Properties(); 
    p.setProperty(PropertyKeys.COLLECTOR_URI, "https://localhost:8088");
    p.setProperty(PropertyKeys.TOKEN, "5c93e5e4-42d4-4997-9f60-e4a11e5cae04");
    return p;
  }
    
    @Test
    public void changeInFlight() throws InterruptedException{
       Thread t =  new Thread(()->{
            while(!Thread.currentThread().isInterrupted()){
                connection.getSettings().setHecEndpointType(
                        Connection.HecEndpoint.RAW_EVENTS_ENDPOINT);
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException ex) {
                    break;
                }
                connection.getSettings().setHecEndpointType(
                        Connection.HecEndpoint.STRUCTURED_EVENTS_ENDPOINT);
            }
        });
       t.setDaemon(true);//so jvm will exit despite this thread running
       t.start();
       super.sendEvents();
    }

    @Override
    protected int getNumEventsToSend() {        
        return 10000000;
    }
    
}
