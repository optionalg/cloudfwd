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

import com.splunk.cloudfwd.http.lifecycle.LifecycleEvent;
import com.splunk.cloudfwd.http.lifecycle.EventBatchResponse;
import com.splunk.cloudfwd.http.lifecycle.LifecycleEventObserver;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author ghendrey
 */
public class CheckpointManager implements LifecycleEventObserver {
  
  private static final Logger LOG = Logger.getLogger(CheckpointManager.class.
          getName());

  //EventBatch callbacks are ordered by EventBatch id lexicographic comparison
  private final NavigableMap<String, EventBatch> orderedEvents = new ConcurrentSkipListMap<>(); //key EventBatch.id, value is EventBatch
  private final Connection connection;
  
  CheckpointManager(Connection c) {
    this.connection = c;
  }
  
  @Override
  public void update(LifecycleEvent e) {
    if (e.getType() == LifecycleEvent.Type.ACK_POLL_OK) {
      EventBatchResponse resp = (EventBatchResponse) e;
      String id = resp.getEvents().getId();
      /*
        System.out.println(
                "MAYBE CALLBACK HIGHWATER for " + id + "(ackId is " + es.
                getEvents().getAckId() + ")");
       */
      acknowledgeHighwaterAndBelow(resp.getEvents());
    }
  }
  
  @Override
  public String toString() {
    return eventBatchWindowStateToString();
  }
  
  public String eventBatchWindowStateToString() {
    StringBuilder sb = new StringBuilder();
    for (EventBatch events : this.orderedEvents.values()) {
      String chan = null == events.getSender() ? "--------------------------------null" : events.
              getSender().getChannel().getChannelId();
      sb.append("chan=").append(chan).append(", seqno=").append(events.
              getId()).append(", ackID=").append(events.getAckId()).append(
              ", acked=").append(events.isAcknowledged() == true ? "1" : "0").
              append("\n");
    }
    return sb.toString();
  }

  //if EventsBatch is the lowest (by ID) then remove it, and consecutive higher keys that have been
  //acknowledged out of order. Return Callback from hightest EventBatch, for which there are no 
  //lower unacknowledged event batches.
  private synchronized void acknowledgeHighwaterAndBelow(EventBatch events) {
    events.setAcknowledged(true);
    //System.out.println("window state: " + eventBatchWindowStateToString());
    if (!this.orderedEvents.containsKey(events.getId())) {
      String msg = "No callback registered for successfully acknowledged ackId: " + events.
              getAckId();
      Logger.getLogger(getClass().getName()).log(Level.SEVERE, msg);
      throw new IllegalStateException(msg);
    }
    FutureCallback cb = this.connection.getCallbacks();
    //todo: maybe schedule acknowledge to be async
    cb.acknowledged(events); //hit the callback to tell the user code that the EventBatch succeeded
    if (!events.getId().equals(this.orderedEvents.firstKey())) { //if this batch isn't the highwater
      //bail because there are unacknowleged EventBatches with lower sequence IDs
      //In other words, highwater hasn't moved
      return;
    }
    slideHighwaterUp(cb); //might call the highwater/checkpoint callback
  }
  
  private void slideHighwaterUp(FutureCallback cb) {
    EventBatch events = null;
    //walk forward in the order of EventBatches, from the tail
    for (Iterator<Map.Entry<String, EventBatch>> iter = this.orderedEvents.
            entrySet().iterator(); iter.hasNext();) {
      Map.Entry<String, EventBatch> e = iter.next();
      events = e.getValue();
      if (events.isAcknowledged()) { //this causes us to remove all *consecutive* acknowledged EventBatch, forward from the tail
        iter.remove(); //remove the callback (we are going to call it now, so no need to track it any longer)
      } else {
        break;
      }
    }
    if (null == events) {
      throw new IllegalStateException(
              "Failed to move highwater mark. No events present.");
    }
    //todo: maybe schedule checkpoint to be async
    cb.checkpoint(events); //only checkpoint the highwater mark. Checkpointing lower ones is redundant.
  }
  
  synchronized void registerInFlightEvents(EventBatch events) {
    EventBatch prev = this.orderedEvents.put(events.getId(), events);
    if (null != prev) {
      throw new IllegalStateException(
              "EventBatch checkpoint already tracked. EventBatch ID is " + events.
              getId());
    }
  }
  
}
