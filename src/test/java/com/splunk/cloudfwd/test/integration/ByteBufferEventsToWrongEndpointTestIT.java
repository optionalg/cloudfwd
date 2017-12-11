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
package com.splunk.cloudfwd.test.integration;

import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.Event;
import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.UnvalidatedByteBufferEvent;
import com.splunk.cloudfwd.error.HecServerErrorResponseException;
import com.splunk.cloudfwd.impl.util.PropertiesFileHelper;
import com.splunk.cloudfwd.test.integration.AbstractReconciliationTest;
import com.splunk.cloudfwd.test.util.BasicCallbacks;
import java.nio.ByteBuffer;
import java.util.Properties;
import org.junit.Test;

/**
 * this test sends raw text events to the events endpoint in order to force an error. Error manifests as failed callback.
 * @author ghendrey
 */
public class ByteBufferEventsToWrongEndpointTestIT extends AbstractReconciliationTest{
  @Override
  protected void setProps(PropertiesFileHelper settings) {
      //intentionally direct text events to /events endpoint
      settings.setHecEndpointType(Connection.HecEndpoint.STRUCTURED_EVENTS_ENDPOINT);
      settings.setEventBatchSize(16000);
      super.eventType = Event.Type.UNKNOWN; //meeans its a Unvalidated* event (we can't tell what content is in it)
  }
    
  @Override
 protected BasicCallbacks getCallbacks() {
     return new BasicCallbacks(getNumEventsToSend()){
           @Override
           protected boolean isExpectedFailureType(Exception e){
                return e instanceof HecServerErrorResponseException && ((HecServerErrorResponseException)e).getCode()==6 &&
                        "Invalid data format".equals(((HecServerErrorResponseException)e).getHecErrorText())&&
                        ((HecServerErrorResponseException)e).getInvalidEventNumber()==0;
          }
           @Override
            public boolean shouldFail(){
              return true;
          }
     };
 }
       
    
 @Override
  protected Event nextEvent(int seqno) {
    Event event = null;
    switch (this.eventType) {
      case UNKNOWN: {
        event = getByteBufferEvent(seqno);
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

  protected void configureConnection(Connection connection) {
    connection.getSettings().setHecEndpointType(
              Connection.HecEndpoint.STRUCTURED_EVENTS_ENDPOINT);
  }  
    
    
  protected Event getByteBufferEvent(int seqno) {
    Event e  = getTimestampedRawEvent(seqno); //intentionally get wrong type
    return new UnvalidatedByteBufferEvent(ByteBuffer.wrap(e.getBytes()), seqno);
  }    
  
  @Override
  protected int getNumEventsToSend() {
    return 100; 
  }  
  
  @Test
  public void test() throws InterruptedException{
      super.sendEvents();
  }
}
