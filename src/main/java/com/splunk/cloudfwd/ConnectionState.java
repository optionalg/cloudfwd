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

import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author ghendrey
 */
public class ConnectionState extends Observable implements Observer {

  //EventBatch callbacks are ordered by EventBatch id lexicographic comparison
  private final NavigableMap<String, BatchCallback> successCallbacks = new ConcurrentSkipListMap<>(); //key EventBatch.id, value is completion callback

  void setSuccessCallback(EventBatch events, Runnable callback) {
    this.successCallbacks.put(events.getId(),
            new BatchCallback(events, callback));
  }
  
  @Override
  public void update(Observable o, Object arg) {
    if (!(arg instanceof AckLifecycleState)) {
      return; //ignore updates we don't care about, like those destined for LoadBalancer
    }
    System.out.println("CONN STATE UPDATE");
    AckLifecycleState es = (AckLifecycleState) arg;
    if (es.getCurrentState() == AckLifecycleState.State.ACK_POLL_OK) {
      String id = es.getEvents().getId();
      Runnable runMe = getCallback(es.getEvents());
      if (null != runMe) {
        runMe.run(); //callback
      }
    }
    ChannelMetrics cm = (ChannelMetrics) o;
    
  }

  //if EventsBatch is the lowest (by ID) then remove it, and consecutive higher keys that have been
  //acknowledged out of order. Return Callback from hightest EventBatch, for which there are no 
  //lower unacknowledged event batches.
  private synchronized Runnable getCallback(EventBatch events) {
    events.setAcknowledged(true);
    if (!this.successCallbacks.containsKey(events.getId())) {
      String msg = "No callback registered for successfully acknowledged ackId: " + events.getAckId();
      Logger.getLogger(getClass().getName()).log(Level.SEVERE, msg);
      throw new IllegalStateException(msg);
    }
    if (events.getId().equals(this.successCallbacks.firstKey())) { //if there is no lower key   
      Runnable highestAckdCallback = this.successCallbacks.get(events.getId()).
              getCallback();
      //walk forward in the order of EventBatches, from the tail
      for (Iterator<Map.Entry<String, BatchCallback>> iter = this.successCallbacks.
              entrySet().iterator(); iter.hasNext();) {
        Map.Entry<String, BatchCallback> e = iter.next();
        if (e.getValue().getEvents().isAcknowledged()) { //this causes us to remove all *consecutive* acknowledged EventBatch, forward from the tail
          highestAckdCallback = e.getValue().getCallback();
          iter.remove(); //remove the callback (we are going to call it now, so no need to track it any longer)
        } else {
          break;
        }
      }
      return highestAckdCallback;
    }
    return null; //no callback returned. There are lower sequence number, unack'd EventBatch outstanding
  }
  
  private static class BatchCallback {
    
    private EventBatch events;
    private Runnable callback;
    
    public BatchCallback(EventBatch events, Runnable callback) {
      this.events = events;
      this.callback = callback;
    }
    
    public EventBatch getEvents() {
      return events;
    }
    
    public void setEvents(EventBatch events) {
      this.events = events;
    }
    
    public Runnable getCallback() {
      return callback;
    }
    
    public void setCallback(Runnable callback) {
      this.callback = callback;
    }
    
  }
  
}
