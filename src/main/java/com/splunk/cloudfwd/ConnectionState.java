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

import com.splunk.cloudfwd.http.LifecycleEvent;
import com.splunk.cloudfwd.http.EventBatch;
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

  private static final Logger LOG = Logger.getLogger(ConnectionState.class.
          getName());

  //EventBatch callbacks are ordered by EventBatch id lexicographic comparison
  private final NavigableMap<String, EventBatch> orderedEvents = new ConcurrentSkipListMap<>(); //key EventBatch.id, value is EventBatch
  private final Connection connection;

  ConnectionState(Connection c) {
    this.connection = c;
  }

  @Override
  public void update(Observable o, Object arg) {
    //System.out.println("ping connectionstate");
    try {
      if (!(arg instanceof LifecycleEvent)) {
        LOG.info("ConnectionState ignoring update of " + arg.getClass().
                getName());
        return; //ignore updates we don't care about, like those destined for LoadBalancer
      }

      LifecycleEvent es = (LifecycleEvent) arg;
      /*
      System.out.println("CONN STATE UPDATE channel=" + es.getSender().
              getChannel());*/
      if (es.getType() == LifecycleEvent.Type.ACK_POLL_OK) {
        String id = es.getEvents().getId();
        /*
        System.out.println(
                "MAYBE CALLBACK HIGHWATER for " + id + "(ackId is " + es.
                getEvents().getAckId() + ")");
        */
        acknowledgeHighwaterAndBelow(es.getEvents());
      }
    } catch (Exception ex) {
      LOG.severe(ex.getMessage());
      ex.printStackTrace();
      throw new RuntimeException(ex.getMessage(), ex);
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
    this.orderedEvents.put(events.getId(), events);
  }

  private static class BatchCallback {

    private EventBatch events;
    private FutureCallback callback;

    public BatchCallback(EventBatch events, FutureCallback callback) {
      this.events = events;
      this.callback = callback;
    }

    public EventBatch getEvents() {
      return events;
    }

    public void setEvents(EventBatch events) {
      this.events = events;
    }

    /**
     * @return the callback
     */
    public FutureCallback getCallback() {
      return callback;
    }

  }

}
