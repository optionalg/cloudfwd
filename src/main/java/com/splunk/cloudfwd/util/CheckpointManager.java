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

import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.EventBatch;
import com.splunk.cloudfwd.http.lifecycle.LifecycleEvent;
import com.splunk.cloudfwd.http.lifecycle.EventBatchResponse;
import com.splunk.cloudfwd.http.lifecycle.LifecycleEventObserver;
import java.util.Iterator;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.SortedMap;
import java.util.TreeMap;
import com.splunk.cloudfwd.ConnectionCallbacks;
import com.splunk.cloudfwd.HecAlreadyAcknowledgedException;
import com.splunk.cloudfwd.HecDuplicateEventException;
import com.splunk.cloudfwd.HecIllegalStateException;

/**
 *
 * @author ghendrey
 */
public class CheckpointManager implements LifecycleEventObserver {

  protected static final Logger LOG = LoggerFactory.getLogger(
          CheckpointManager.class.getName());

  volatile private  SortedMap<Comparable, EventBatch> orderedEvents = new TreeMap<>(); //key EventBatch.id, value is EventBatch
  private final Connection connection;

  CheckpointManager(Connection c) {
    this.connection = c;
  }

  @Override
  public void update(LifecycleEvent e) {
    if (e.getType() == LifecycleEvent.Type.ACK_POLL_OK) {
      EventBatchResponse resp = (EventBatchResponse) e;
      
      Comparable id = resp.getEvents().getId();
        System.out.println(
                "MAYBE CALLBACK HIGHWATER for " + id );
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

  //if EventsBatch is the lowest (by ID) then cancelEventTrackers it, and consecutive higher keys that have been
  //acknowledged out of order. Return Callback from hightest EventBatch, for which there are no
  //lower unacknowledged event batches.
  private synchronized void acknowledgeHighwaterAndBelow(EventBatch events) {
    //Do not under penalty of death remove this commented sys out line below :-)
    //very useful for debugging...
    //System.out.println("window state: " + eventBatchWindowStateToString());
    if (orderedEvents.isEmpty() || !orderedEvents.containsKey(events.getId())) {
      String msg = "No callback registered for successfully acknowledged event batch id: " + events.
              getId() + ". This can happen if event has been resent by DeadChannelDetector";
      LOG.warn(msg);
      return;
    }
    ConnectionCallbacks cb = this.connection.getCallbacks();
    if (!events.getId().equals(this.orderedEvents.firstKey())) { //if this batch isn't the highwater
      //bail because there are unacknowleged EventBatches with lower sequence IDs
      //In other words, highwater hasn't moved
      return;
    }
    cb.acknowledged(events); //hit the callback to tell the user code that the EventBatch succeeded
    events.setAcknowledged(true);
    slideHighwaterUp(cb, events); //might call the highwater/checkpoint callback
    //musn't call cb.acknowledged until after we slideHighwater, because calling cb.acknowleged
    //will cause the CallbackInterceptor in Connection to cancel tracking of the EventBatch. Then
    //then slideHighwater won't work

  }

  private synchronized void slideHighwaterUp(ConnectionCallbacks cb,
          EventBatch events) {
    System.out.println(this.eventBatchWindowStateToString());
    if (this.orderedEvents.isEmpty()) {
      String msg = "Failed to move highwater mark. No events tracked.";
      LOG.error(msg);
      throw new HecIllegalStateException(msg,
              HecIllegalStateException.Type.NO_EVENTS_TO_CHECKPOINT);
    }
    if (this.orderedEvents.isEmpty()) {
      String msg = "Failed to move highwater mark. Event not tracked: " + events.getId();
      LOG.error(msg);
      throw new HecIllegalStateException(msg,
              HecIllegalStateException.Type.EVENT_NOT_PRESENT_IN_CHECKPOINT_SET);
    }
    if(!events.isAcknowledged()){
      String msg = "Attempt to recompute highwater mark on unacknowledged EventBatch: " + events.getId();
      LOG.error(msg);
      throw new HecIllegalStateException(msg,
              HecIllegalStateException.Type.EVENT_NOT_ACKNOWLEDGED_BUT_HIGHWATER_RECOMPUTED);
    }
    EventBatch acknowledgedEvents = null;
    //walk forward in the order of EventBatches, from the tail
    for (Iterator<Map.Entry<Comparable, EventBatch>> iter = this.orderedEvents.
            entrySet().iterator(); iter.hasNext();) {
      Map.Entry<Comparable, EventBatch> e = iter.next();
      //this causes us to cancelEventTrackers all *consecutive* acknowledged EventBatch, forward from the tail
      if (e.getValue().isAcknowledged()) {
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
    //event id must not be below the highwater mark
    if (!orderedEvents.isEmpty() && events.getId().compareTo(orderedEvents.
            firstKey()) < 0) {
      String msg = "EventBatch already acknowldeged. EventBatch ID is " + events.getId();
      LOG.error(msg);
      throw new HecAlreadyAcknowledgedException(msg);
    }
    EventBatch prev = this.orderedEvents.put(events.getId(), events);
    //events.registerEventTracker(this);
    LOG.trace("Registering EventBatch {} on {}", events.getId(), this.hashCode());
    if (null != prev) {
      String msg = "EventBatch checkpoint already tracked. EventBatch ID is " + events.
              getId();
      LOG.error(msg);
      throw new HecDuplicateEventException(msg);
    }
  }

  
  

  public synchronized void cancel(EventBatch events) {
    //since highwater may have removed the key, we cant make any inference about correcteness based on whether the 
    //key was or was not still in the orderedEvents.
    this.orderedEvents.remove(events.getId()); 
//    
//    LOG.trace("deregister event batch {} on {}", events.getId(), this.hashCode());
//    if (null == prev) {
//      String msg = "Attempt to deregister unregistered EventBatch. EventBatch ID is " + events.
//              getId();
//      LOG.error(msg);
//      throw new IllegalStateException(msg);
//    }
     
  }

  /*
  @Override
  public boolean isInternal() {
    return true;
  }
*/
}
