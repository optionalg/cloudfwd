package com.splunk.cloudfwd.test.mock;

import com.splunk.cloudfwd.ConnectionSettings;
import com.splunk.cloudfwd.Event;
import com.splunk.cloudfwd.LifecycleEvent;
import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import com.splunk.cloudfwd.error.HecServerErrorResponseException;
import com.splunk.cloudfwd.test.util.AbstractConnectionTest;
import com.splunk.cloudfwd.test.util.BasicCallbacks;
import com.splunk.cloudfwd.impl.sim.errorgen.acks.OutOfOrderAckIDFailEndpoints;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

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
public class OutOfOrderAckIdWithFailTest extends AbstractConnectionTest {

    final int n = 10;
    boolean fail = true;

    
    @Override
    protected BasicCallbacks getCallbacks() {
      return new BasicCallbacks(n){
           
        @Override
          public boolean shouldFail(){
          return true;
        }          
        @Override
        protected boolean isExpectedFailureType(Exception e){
          return (e instanceof HecServerErrorResponseException &&
              ((HecServerErrorResponseException)e).getLifecycleType()==LifecycleEvent.Type.INDEXER_IN_DETENTION);
        }    
      };
    }

    
    @Test
    public void testAckAndFail() throws InterruptedException{
        sendEvents();
    }
    
    @Override
    protected void sendEvents() throws InterruptedException, HecConnectionTimeoutException {
      int expected = getNumEventsToSend();
      if(expected <= 0){
          return;
      }
      try {
          LOG.trace(
                "SENDING EVENTS WITH CLASS GUID: " + TEST_CLASS_INSTANCE_GUID
                        + "And test method GUID " + testMethodGUID);
          for (int i = 0; i < expected; i++) {
              if (i % (n/4) == 0) {
                fail = !fail; //0-25:ok, 26-50: fail, 51-75:fail, 76-100:ok
                OutOfOrderAckIDFailEndpoints.toggleFail(fail);
              }            
              Event event = nextEvent(i + 1);
              LOG.trace("Send event {} i={}", event.getId(), i);
              connection.send(event);
          }
      } catch(Exception e) {
        // expected exception
        Assert.assertTrue(e instanceof HecConnectionTimeoutException);
        LOG.trace("In Test caught expected exception on Connection.send(): {} with message {}", e, e.getMessage());
      }
      connection.close(); //will flush
  
      this.callbacks.await(10, TimeUnit.MINUTES);
      this.callbacks.checkFailures();
      this.callbacks.checkWarnings();
    }

    @Override
    protected void configureProps(ConnectionSettings settings) {
        settings.setMockHttpClassname("com.splunk.cloudfwd.impl.sim.errorgen.acks.OutOfOrderAckIDFailEndpoints");
        settings.setBlockingTimeoutMS(3000);
        settings.setUnresponsiveMS(-1); //no dead channel detection
        settings.setMaxTotalChannels(4);
        settings.setAckTimeoutMS(60000); //we don't want the ack timout kicking in
        // checkpointing
        settings.setCheckpointEnabled(true);
    }

    @Override
    protected int getNumEventsToSend() {
        return n;
    }
}
