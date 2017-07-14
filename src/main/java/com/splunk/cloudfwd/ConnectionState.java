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
package com.splunk.cloudfwd;

import com.splunk.logging.AckLifecycleState;
import com.splunk.logging.ChannelMetrics;
import com.splunk.logging.EventBatch;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.logging.Level;
import java.util.logging.Logger;
/**
 *
 * @author ghendrey
 */
public class ConnectionState extends Observable implements Observer{
  private final Map<Long, Runnable> successCallbacks = new ConcurrentSkipListMap<>();
  
  void setSuccessCallback(EventBatch events, Runnable callback){
    this.successCallbacks.put(events.getId(), callback);
  }
  
  @Override
  public void update(Observable o, Object arg) {
    if(!(arg instanceof AckLifecycleState)){
      return; //ignore updates we don't care about, like those destined for LoadBalancer
    }
    System.out.println("CONN STATE UPDATE");
    AckLifecycleState es = (AckLifecycleState) arg;
    if(es.getCurrentState()== AckLifecycleState.State.ACK_POLL_OK){
      long id  = es.getEvents().getId();
      Runnable runMe = successCallbacks.get(id);
      if(null == runMe){
        String msg = "failed to find callback for successfully acknowledged ackId: " + es.getEvents().getAckId();
        Logger.getLogger(getClass().getName()).log(Level.SEVERE, msg);
        throw new IllegalStateException(msg);
      }
      runMe.run(); //callback
    }
     ChannelMetrics cm = (ChannelMetrics) o;
     
  }
  
}
