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
package com.splunk.cloudfwd.impl.http;

import com.splunk.cloudfwd.impl.EventBatchImpl;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.splunk.cloudfwd.error.HecIllegalStateException;
import com.splunk.cloudfwd.impl.http.lifecycle.EventBatchResponse;
import com.splunk.cloudfwd.LifecycleEvent;
import com.splunk.cloudfwd.impl.util.EventTracker;
import com.splunk.cloudfwd.impl.ConnectionImpl;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keeps track of acks that we are waiting for success on. Updates
 * ChannelMetrics when success is received on an ackId.
 *
 * @author ghendrey
 */
public class AcknowledgementTracker implements EventTracker {

  private Logger LOG = LoggerFactory.getLogger(AcknowledgementTracker.class.getName());

  private final static ObjectMapper jsonMapper = new ObjectMapper();
  private final Map<Long, EventBatchImpl> polledAcksByAckId = new ConcurrentHashMap<>(); //key ackID
 // private final Map<Long, EventBatchImpl> polledAcksByEvent = new ConcurrentHashMap<>(); //key ackID
  //private final Map<Comparable, EventBatchImpl> eventBatches = new ConcurrentHashMap<>();
  private final HttpSender sender; 
  private volatile boolean dead;
  
  AcknowledgementTracker(HttpSender sender) {
    this.sender = sender;
  }

    /**
     * When a session cookie violation occurs, the ack tracker needs to be immediately killed
     */
    public void kill(){
        this.polledAcksByAckId.clear();
        dead = true;
    }
    
  @Override
  public void cancel(EventBatchImpl e) {
      if(null != e.getAckId()){
            polledAcksByAckId.remove(e.getAckId());
      }
  }

  /**
   * Returns the request whose string is posted to acks endpoint. But caller
   * should check AckRequest.isEmpty first
   *
   * @return
   */
  public AckRequest getAckRequest() {
    return new AckRequest(polledAcksByAckId.keySet());
  }

  public boolean isEmpty() {
    return this.sender.getChannel().isEmpty();
  }

  public void preEventPost(EventBatchImpl events) {
    events.registerEventTracker(this);
  }


  public void handleEventPostResponse(EventPostResponseValueObject epr,
          EventBatchImpl events) {
      if(dead){
          return;
      }
    Long ackId = epr.getAckId();
    polledAcksByAckId.put(ackId, events);
  }

  public void handleAckPollResponse(AckPollResponseValueObject apr) {
    if(dead){
        return;
    }
    EventBatchImpl events = null;
    try {
      Collection<Long> succeeded = apr.getSuccessIds();
      LOG.debug("ConnectionImpl={} channel={} num_success_ackIds={} num_outstanding_ackIds={}. Success acked ids: {}", 
        sender.getConnection(), sender.getChannel(), succeeded.size(), apr.getAcks().size(), succeeded);
      if (succeeded.isEmpty()) {
        return;
      }
      for (long ackId : succeeded) {
        events = polledAcksByAckId.get(ackId);
        if (null == events) {
          LOG.warn(
                  "Got acknowledgement on ackId: {} but we're no long tracking that ackId on channel={}",
                  ackId, sender.getChannel());
          continue;
        }
        if(events.isAcknowledged()){
            LOG.warn("will not ack already acked: {}", events);
            continue; //can happen when when sticky session violated
        }
        //System.out.println("got ack on channel=" + events.getSender().getChannel() + ", seqno=" + events.getId() +", ackid=" + events.getAckId());
        //events.getAckId can be null if the event is being resent by DeadChannel detector 
        //and EventBatchImpl.prepareForResend has been called
        if (events.getAckId() != null && ackId != events.getAckId()) {
            String msg = "ackId mismatch key ackID=" + ackId + " existing: " + events + " on channel="+ sender.getChannel();
            LOG.warn(msg);
            continue; //conceivable from sticky session violation
  //          throw new HecIllegalStateException(msg,
  //                  HecIllegalStateException.Type.ACK_ID_MISMATCH);
        }
        events.setAcknowledged(true);    

        this.sender.getChannelMetrics().update(new EventBatchResponse(
                LifecycleEvent.Type.ACK_POLL_OK, 200, "N/A", //we don't care about the message body on 200
                events,sender.getBaseUrl()));
        polledAcksByAckId.remove(ackId);
      }
    } catch (Exception e) {
      LOG.error("caught exception in handleAckPollResponse: " + e.getMessage(),
              e);
      sender.getConnection().getCallbacks().failed(events, e);
    }
  }

  ChannelMetrics getChannelMetrics() {
    return this.sender.getChannelMetrics();
  }

  public Collection<Long> getPostedButUnackedEvents() {
    return Collections.unmodifiableSet(polledAcksByAckId.keySet());
  }

  /**
   * Thus class is used to formulate the body of the HTTP ack polling request
   */
  public static class AckRequest {

    Set<Long> ackIds = new LinkedHashSet<>();

    public AckRequest(Set<Long> ackIds) {
      //take a copy, otherwise the ack id set can empty before we post it and post empty ack set is illegal
      this.ackIds.addAll(ackIds);
    }

    /**
     * @return the empty
     */
    public boolean isEmpty() {
      return ackIds.isEmpty();
    }

    /**
     * @return the request POST content for ack polling like {"acks":[1,2,3]}
     */
    @Override
    public String toString() {
      try {
        Map json = new HashMap();
        json.put("acks", this.ackIds); //{"acks":[1,2,3...]} THIS IS THE MESSAGE WE POST TO HEC
        return jsonMapper.writeValueAsString(json);
      } catch (JsonProcessingException ex) {
        throw new RuntimeException(ex.getMessage(), ex);
      }
    }

  }

  public void setLogger(ConnectionImpl c) {
    this.LOG = c.getLogger(AcknowledgementTracker.class.getName());
  }

}
