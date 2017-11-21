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
package com.splunk.cloudfwd.impl.util;

import com.splunk.cloudfwd.impl.ConnectionImpl;
import com.splunk.cloudfwd.impl.EventBatchImpl;
import com.splunk.cloudfwd.LifecycleEvent;
import com.splunk.cloudfwd.impl.http.lifecycle.EventBatchResponse;
import com.splunk.cloudfwd.impl.http.lifecycle.LifecycleEventObserver;
import java.util.Iterator;
import java.util.Map;
import org.slf4j.Logger;
import java.util.SortedMap;
import java.util.TreeMap;
import com.splunk.cloudfwd.ConnectionCallbacks;
import com.splunk.cloudfwd.error.HecIllegalStateException;
import com.splunk.cloudfwd.error.HecConnectionStateException;
import static com.splunk.cloudfwd.error.HecIllegalStateException.Type.EVENT_ON_FLIGHT_BUT_HIGHWATER_RECOMPUTED;
import static com.splunk.cloudfwd.LifecycleEvent.Type.ACK_POLL_OK;

/**
 *
 * @author ghendrey
 */
public class CheckpointManager implements LifecycleEventObserver {
    private Logger LOG;
    volatile private SortedMap<Comparable, EventBatchImpl> orderedEvents = new TreeMap<>(); //key EventBatchImpl.id, value is EventBatchImpl
    private final ConnectionImpl connection;
    private Comparable checkpoint;
    private boolean enabled;

    public CheckpointManager(ConnectionImpl c) {
        this.LOG = c.getLogger(CheckpointManager.class.getName());
        this.connection = c;
        this.enabled = c.getSettings().isCheckpointEnabled();
    }

    @Override
    public void update(LifecycleEvent e) {
        if (e.getType() == ACK_POLL_OK) {
            EventBatchResponse resp = (EventBatchResponse) e;
            Comparable id = resp.getEvents().getId();
            if (enabled) {
                acknowledgeHighwaterAndBelow(resp.getEvents());
            } else { //if checkpoints not enabled, we simple call both acknowledged and checkpoint without regard for highwater
                ConnectionCallbacks cb = this.connection.getCallbacks();
                cb.acknowledged(resp.getEvents());
                cb.checkpoint(resp.getEvents());
            }
        }
    }

    @Override
    public String toString() {
        return this.orderedEvents.toString();
    }

    //if EventsBatch is the lowest (by ID) then cancelEventTrackers it, and consecutive higher keys that have been
    //acknowledged out of order. Return Callback from hightest EventBatchImpl, for which there are no
    //lower unacknowledged event batches.
    private synchronized void acknowledgeHighwaterAndBelow(EventBatchImpl events) {
        ConnectionCallbacks cb = this.connection.getCallbacks();
        //callback acknowledge - must do this before bailing if events isn't highwater
        cb.acknowledged(events); //hit the callback to tell the user code that the EventBatchImpl succeeded 
        release(events);
    }

    public synchronized void release(EventBatchImpl events) {
      // we need an event batch to checkpoint
      if (events == null) {
        return;
      }

      ConnectionCallbacks cb = this.connection.getCallbacks();

      //Do not under penalty of death remove this commented sys out line below :-)
      //very useful for debugging...
      LOG.debug("handling ack/failed-checkpoint-logic for event {}", events);
      LOG.trace("window state: {}", this);
      if (orderedEvents.isEmpty() || !orderedEvents.
              containsKey(events.getId())) {
          //this can happen when the on-demand ack-poll overlaps with the periodic ack poll,
          String msg = "No callback registered for successfully acknowledged event batch id: " + events.
                  getId() + ". This can happen when an  ack-poll comes back after success checkpoint";
          LOG.debug(msg);
          return;
      }

      if (!events.getId().equals(this.orderedEvents.firstKey())) { //if this batch isn't the highwater
          //bail because there are on-flight EventBatches with lower sequence IDs
          //In other words, highwater hasn't moved
          LOG.debug("Cant slide {}", events.getId());
          return;
      }
      slideHighwaterUp(cb, events);
    }

    private synchronized void slideHighwaterUp(ConnectionCallbacks cb,
            EventBatchImpl events) {
        LOG.debug("trying to slide highwater for {}", events.getId());
        if (!events.isAcknowledged() && !events.isFailed()) {
            String msg = "Attempt to recompute highwater mark on on-flight EventBatch: " + events.
                    getId();
            throw new HecIllegalStateException(msg,
                    EVENT_ON_FLIGHT_BUT_HIGHWATER_RECOMPUTED);
        }
        EventBatchImpl ackedOrFailedEvents = null;
        //walk forward in the order of EventBatches, from the tail
        for (Iterator<Map.Entry<Comparable, EventBatchImpl>> iter = this.orderedEvents.
                entrySet().iterator(); iter.hasNext();) {
            Map.Entry<Comparable, EventBatchImpl> e = iter.next();
            //this causes us to cancelEventTrackers all *consecutive* acknowledged EventBatchImpl, forward from the tail
            if (e.getValue().isAcknowledged() || e.getValue().isFailed()) {
                iter.remove(); //remove the callback (we are going to call it now, so no need to track it any longer)
                ackedOrFailedEvents = e.getValue(); //hang on to highest acknowledged or failed batch id
            } else {
                break;
            }
        }

        //todo: maybe schedule checkpoint to be async
        if (null != ackedOrFailedEvents) {
            LOG.info("CHECKPOINT at {}", ackedOrFailedEvents.getId());
            cb.checkpoint(ackedOrFailedEvents); //only checkpoint the highwater mark. Checkpointing lower ones is redundant.
            checkpoint = ackedOrFailedEvents.getId();
        }
    }

    synchronized void registerEventBatch(EventBatchImpl events, boolean forced) {
        if (!enabled) {
            return;
        }
        //event id must not be below the highwater mark
        if (null != checkpoint && events.getId().compareTo(checkpoint) <= 0) {
            String msg = "EventBatch already handled (acknowledged or failed). EventBatch ID is " + events.
                    getId() + " but checkpoint at " + checkpoint;
            throw new HecConnectionStateException(msg,
                    HecConnectionStateException.Type.ALREADY_HANDLED);
        }
        EventBatchImpl prev = this.orderedEvents.get(events.getId());
        if (null != prev) {
            LOG.trace("Existing EventBatch for is {}", prev);
        }
        //Forced happens when we DeadChannelDetector needs to resend events
        //Retriable EventBatchImpl is one that has not yet received EVENT_POST_OK
        if (null != prev && !forced && !prev.isRetriable()) {
            String msg = "Attempt to resend EventBatch that is not retriable because previous state was: {}  " + prev.
                    getState();
            throw new HecConnectionStateException(msg,
                    HecConnectionStateException.Type.ALREADY_SENT);
        }

        this.orderedEvents.put(events.getId(), events);
    }

    public synchronized void cancel(Comparable id) {
        if (!enabled) {
            return;
        }
        //since highwater may have removed the key, we cant make any inference about correcteness based on whether the 
        //key was or was not still in the orderedEvents.
        LOG.info("cancelled/removed checkpoint for id {}", id);
        this.orderedEvents.remove(id);
//    
//    LOG.trace("deregister event batch {} on {}", events.getId(), this.hashCode());
//    if (null == prev) {
//      String msg = "Attempt to deregister unregistered EventBatchImpl. EventBatchImpl ID is " + events.
//              getId();
//      LOG.error(msg);
//      throw new IllegalStateException(msg);
//    }

    }

}
