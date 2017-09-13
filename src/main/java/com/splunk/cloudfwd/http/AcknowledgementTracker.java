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
import com.splunk.cloudfwd.HecIllegalStateException;
import com.splunk.cloudfwd.HecConnectionStateException;
import com.splunk.cloudfwd.http.lifecycle.EventBatchResponse;
import com.splunk.cloudfwd.http.lifecycle.LifecycleEvent;
import com.splunk.cloudfwd.util.EventTracker;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
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

  private static final Logger LOG = LoggerFactory.getLogger(
          AcknowledgementTracker.class.getName());

  private final static ObjectMapper jsonMapper = new ObjectMapper();
  private final Map<Long, EventBatch> polledAcks = new ConcurrentHashMap<>(); //key ackID
  //private final Map<Comparable, EventBatch> eventBatches = new ConcurrentHashMap<>();
  private final HttpSender sender;

  AcknowledgementTracker(HttpSender sender) {
    this.sender = sender;
  }

  @Override
  public void cancel(EventBatch e) {
    //if (null != eventBatches.remove(e.getId())) {
      //hunt for it in the polledAcks
      for (Iterator<Map.Entry<Long, EventBatch>> it = polledAcks.entrySet().
              iterator(); it.hasNext();) {
        Map.Entry<Long, EventBatch> entry = it.next();
        if (e.getId() == entry.getValue().getId()) {
          it.remove();
        }
      }
    //}
  }

  /**
   * Returns the request whose string is posted to acks endpoint. But caller
   * should check AckRequest.isEmpty first
   *
   * @return
   */
  public AckRequest getAckRequest() {
    return new AckRequest(polledAcks.keySet());
  }

  public boolean isEmpty() {
    return this.sender.getChannel().isEmpty();
  }

  public void preEventPost(EventBatch events) {
    events.registerEventTracker(this);
    /*
    if (null != this.eventBatches.put(events.getId(), events)) {
      throwIllegalStateException(events);
    }  
    */
  }

  private void throwIllegalStateException(EventBatch batch) {
    String msg = "Attempt to send EventBatch that is still pending acknowledgement:  " + batch;
    LOG.warn(msg);
    throw new HecConnectionStateException(msg,
            HecConnectionStateException.Type.ALREADY_SENT);
  }

  public void handleEventPostResponse(EventPostResponseValueObject epr,
          EventBatch events) {
    Long ackId = epr.getAckId();
    polledAcks.put(ackId, events);
  }

  public void handleAckPollResponse(AckPollResponseValueObject apr) {
    EventBatch events = null;
    try {
      Collection<Long> succeeded = apr.getSuccessIds();
      LOG.info("Channel:{} success acked ids: {}", sender.getChannel(),
              succeeded);
      if (succeeded.isEmpty()) {
        return;
      }
      for (long ackId : succeeded) {
        events = polledAcks.get(ackId);
        events.setAcknowledged(true);
        if (null == events) {
          LOG.warn(
                  "Got acknowledgement on ackId: {} but we're no long tracking that ackId",
                  ackId);
          return;
        }
        //System.out.println("got ack on channel=" + events.getSender().getChannel() + ", seqno=" + events.getId() +", ackid=" + events.getAckId());
        //events.getAckId can be null if the event is being resent by DeadChannel detector 
        //and EventBatch.prepareForResend has been called
        if (events.getAckId() != null && ackId != events.getAckId()) {
          String msg = "ackId mismatch key ackID=" + ackId + " recordedAckId=" + events.
                  getAckId();
          LOG.error(msg);
          throw new HecIllegalStateException(msg,
                  HecIllegalStateException.Type.ACK_ID_MISMATCH);
        }

        this.sender.getChannelMetrics().update(new EventBatchResponse(
                LifecycleEvent.Type.ACK_POLL_OK, 200, "N/A", //we don't care about the message body on 200
                events,sender.getBaseUrl()));
        //eventBatches.remove(events.getId());
        polledAcks.remove(ackId);
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
    return Collections.unmodifiableSet(polledAcks.keySet());
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
        LOG.error(ex.getMessage(), ex);
        throw new RuntimeException(ex.getMessage(), ex);
      }
    }

  }

}
