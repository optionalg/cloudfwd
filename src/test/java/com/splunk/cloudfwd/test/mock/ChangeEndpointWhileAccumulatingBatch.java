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
package com.splunk.cloudfwd.test.mock;

import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.error.HecConnectionStateException;
import static com.splunk.cloudfwd.error.HecConnectionStateException.Type.WRONG_EVENT_FORMAT_FOR_ENDPOINT;

import com.splunk.cloudfwd.impl.util.PropertiesFileHelper;
import com.splunk.cloudfwd.test.util.AbstractConnectionTest;
import java.util.Properties;
import org.junit.Test;

/**
 * This test starts a thread that switches the connection settings between /raw and /events endpoints 
 * once every 5 seconds. An exception should be thrown by the batch as soon as the events added to it switch
 * from one known type to another.
 * @author ghendrey
 */
public class ChangeEndpointWhileAccumulatingBatch extends AbstractConnectionTest{

  protected void setProps(PropertiesFileHelper settings) {
    settings.setEventBatchSize(1024*1024);
  }
    
    @Test
    public void changeInFlight() throws InterruptedException{
       Thread t =  new Thread(()->{
            while(!Thread.currentThread().isInterrupted()){
                connection.getSettings().setHecEndpointType(
                        Connection.HecEndpoint.RAW_EVENTS_ENDPOINT);
                try {
                    Thread.sleep(1000);
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
       t.interrupt();
    }
    
  @Override
  protected boolean isExpectedSendException(Exception e) {
    return e instanceof HecConnectionStateException 
            && ((HecConnectionStateException)e).getType()==WRONG_EVENT_FORMAT_FOR_ENDPOINT;
  }

  @Override
  protected boolean shouldSendThrowException() {
      return true;
  }

    @Override
    protected int getNumEventsToSend() {        
        return 1000000; //test will fail as expected long before we send this many events
    }
    
}
