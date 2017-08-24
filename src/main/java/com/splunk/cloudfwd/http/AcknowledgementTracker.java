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
package com.splunk.cloudfwd.http;

import com.splunk.cloudfwd.EventBatch;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.splunk.cloudfwd.http.lifecycle.EventBatchResponse;
import com.splunk.cloudfwd.http.lifecycle.LifecycleEvent;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Keeps track of acks that we are waiting for success on. Updates
 * ChannelMetrics every time an ackId is created, and also when success is
 * received on an ackId. This is not really a window in the sense of a sliding
 * window but "window" seems apropos to describe it.
 *
 * @author ghendrey
 */
public class AcknowledgementTracker {

  private static final Logger LOG = Logger.getLogger(
          AcknowledgementTracker.class.getName());
  private final static ObjectMapper jsonMapper = new ObjectMapper();
  private final IdTracker idTracker = new IdTracker();
  private final HttpSender sender;

  AcknowledgementTracker(HttpSender sender) {
    this.sender = sender;
  }

  @Override
  public String toString() {

    try {
      Map json = new HashMap();
      json.put("acks", Collections.unmodifiableSet(idTracker.polledAcks.keySet())); //{"acks":[1,2,3...]} THIS IS THE MESSAGE WE POST TO HEC
      return jsonMapper.writeValueAsString(json); //this class itself marshals out to {"acks":[id,id,id]}
    } catch (JsonProcessingException ex) {
      Logger.getLogger(AcknowledgementTracker.class.getName()).log(Level.SEVERE,
              null, ex);
      throw new RuntimeException(ex.getMessage(), ex);
    }

  }

  public boolean isEmpty() {
    return this.sender.getChannel().isEmpty();
  }

  public void preEventPost(EventBatch batch) {
    //cannot be synchrnoized! Will deadlock. Fortunately does no harm not 
    //to synchronize
    //synchronized (idTracker) {
      this.idTracker.postedEventBatches.put(batch.getId(), batch);  //track what we attempt to post, so in case fail we can try again  
    //}
  }

  public void handleEventPostResponse(EventPostResponseValueObject epr,
          EventBatch events) {
    Long ackId = epr.getAckId();
    //System.out.println("handler event post response for ack " + ackId);
    synchronized (idTracker) {
      EventBatch removed = idTracker.postedEventBatches.remove(events.getId()); //we are now sure the server reveived the events POST
      if (null == removed) {
        String msg = "failed to track event batch " + events.getId();
        LOG.severe(msg);
        throw new RuntimeException(msg);
      }
      idTracker.polledAcks.put(ackId, events);
    }
    //System.out.println("Tracked ackIDs on client "+this.hashCode()+": "+polledAcks.keySet());
  }

  public void handleAckPollResponse(AckPollResponseValueObject apr) {
    EventBatch events = null;
    try {
      Collection<Long> succeeded = apr.getSuccessIds();
      System.out.println("success acked ids: " + succeeded);
      if (succeeded.isEmpty()) {
        return;
      }
      synchronized (idTracker) {
        for (long ackId : succeeded) {
          events = idTracker.polledAcks.get(ackId);
          if (null == events) {
            Logger.getLogger(getClass().getName()).log(Level.SEVERE,
                    "Unable to find EventBatch in buffer for successfully acknowledged ackId: {0}",
                    ackId);
          }
          //System.out.println("got ack on channel=" + events.getSender().getChannel() + ", seqno=" + events.getId() +", ackid=" + events.getAckId());
          if (ackId != events.getAckId()) {
            String msg = "ackId mismatch key ackID=" + ackId + " recordedAckId=" + events.
                    getAckId();
            LOG.severe(msg);
            throw new IllegalStateException(msg);
          }

          this.sender.getChannelMetrics().update(new EventBatchResponse(
                  LifecycleEvent.Type.ACK_POLL_OK, 200, "why do you care?",
                  events));
        }
        //System.out.println("polledAcks was " + polledAcks.keySet());
        idTracker.polledAcks.keySet().removeAll(succeeded);
        //System.out.println("polledAcks now " + polledAcks.keySet());
      }
    } catch (Exception e) {
      LOG.severe("caught exception in handleAckPollResponse: " + e.getMessage());
      sender.getConnection().getCallbacks().failed(events, e);
    }
  }

  ChannelMetrics getChannelMetrics() {
    return this.sender.getChannelMetrics();
  }

  public Collection<Long> getPostedButUnackedEvents() {
    return Collections.unmodifiableSet(idTracker.polledAcks.keySet());
  }

  //techically we need to synchronize this, and the method handleAckPollResponse
  public Collection<EventBatch> getAllInFlightEvents() {
    List<EventBatch> events = new ArrayList<>();
    synchronized (idTracker) {
      events.addAll(idTracker.postedEventBatches.values());
      events.addAll(idTracker.polledAcks.values());
    }
    return events;
  }

  private static class IdTracker {

    public final Map<Long, EventBatch> polledAcks = new ConcurrentHashMap<>(); //key ackID
    public final Map<Comparable, EventBatch> postedEventBatches = new ConcurrentHashMap<>();//key EventBatch ID

  }

}
