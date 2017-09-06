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
package com.splunk.cloudfwd.util;

import com.splunk.cloudfwd.EventBatch;
import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.EventBatch;
import com.splunk.cloudfwd.http.lifecycle.LifecycleEvent;
import com.splunk.cloudfwd.http.lifecycle.EventBatchResponse;
import com.splunk.cloudfwd.http.lifecycle.LifecycleEventObserver;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.SortedMap;
import java.util.TreeMap;
import com.splunk.cloudfwd.ConnectionCallbacks;

/**
 *
 * @author ghendrey
 */
public class CheckpointManager implements LifecycleEventObserver {

  private static final Logger LOG = Logger.getLogger(CheckpointManager.class.
          getName());

  private final SortedMap<Comparable, EventBatch> orderedEvents = new TreeMap<>(); //key EventBatch.id, value is EventBatch
  private final Connection connection;

  CheckpointManager(Connection c) {
    this.connection = c;
  }

  @Override
  public void update(LifecycleEvent e) {
    if (e.getType() == LifecycleEvent.Type.ACK_POLL_OK) {
      EventBatchResponse resp = (EventBatchResponse) e;
      /*
      Comparable id = resp.getEvents().getId();
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
      /*
      String chan = null == events.getSender() ? "--------------------------------null" : events.
              getSender().getChannel().getChannelId();
      sb.append("chan=").append(chan)
       */
      sb.append("seqno=").append(events.
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
    //Do not under penalty of death remove ths commented sys out line below :-)
    //very useful for debugging...
    //System.out.println("window state: " + eventBatchWindowStateToString());
    if (!this.orderedEvents.containsKey(events.getId())) {
      String msg = "No callback registered for successfully acknowledged ackId: " + events.
              getAckId() + ". This can happen if event has been resent by DeadChannelDetector";
      Logger.getLogger(getClass().getName()).log(Level.WARNING, msg);
    }
    ConnectionCallbacks cb = this.connection.getCallbacks();
    //todo: maybe schedule acknowledge to be async
    cb.acknowledged(events); //hit the callback to tell the user code that the EventBatch succeeded
    if (!events.getId().equals(this.orderedEvents.firstKey())) { //if this batch isn't the highwater
      //bail because there are unacknowleged EventBatches with lower sequence IDs
      //In other words, highwater hasn't moved
      return;
    }
    slideHighwaterUp(cb); //might call the highwater/checkpoint callback
  }

  private synchronized void slideHighwaterUp(ConnectionCallbacks cb) {
    if (this.orderedEvents.isEmpty()) {
      String msg = "Failed to move highwater mark. No events present.";
      LOG.severe(msg);
      throw new IllegalStateException(msg);
    }
    EventBatch acknowledgedEvents = null;
    //walk forward in the order of EventBatches, from the tail
    for (Iterator<Map.Entry<Comparable, EventBatch>> iter = this.orderedEvents.
            entrySet().iterator(); iter.hasNext();) {
      Map.Entry<Comparable, EventBatch> e = iter.next();
      if (e.getValue().isAcknowledged()) { //this causes us to remove all *consecutive* acknowledged EventBatch, forward from the tail
        iter.remove(); //remove the callback (we are going to call it now, so no need to track it any longer)
        acknowledgedEvents = e.getValue(); //hang on to highest acknowledged batch id
      } else {
        break;
      }
    }

    //todo: maybe schedule checkpoint to be async
    if (null != acknowledgedEvents) {
      cb.checkpoint(acknowledgedEvents); //only checkpoint the highwater mark. Checkpointing lower ones is redundant.
    }
  }

  synchronized void registerInFlightEvents(EventBatch events) {
    EventBatch prev = this.orderedEvents.put(events.getId(), events);
    if (null != prev) {
      String msg = "EventBatch checkpoint already tracked. EventBatch ID is " + events.
              getId();
      LOG.severe(msg);
      throw new IllegalStateException(msg);
    }
  }
  
  public synchronized void deRegisterInFlightEvents(EventBatch events) {
    EventBatch prev = this.orderedEvents.remove(events.getId());
    if (null == prev) {
      String msg = "Attempt to deregister unregistered EventBatch. EventBatch ID is " + events.
              getId();
      LOG.severe(msg);
      throw new IllegalStateException(msg);
    }
  }  
  
}
