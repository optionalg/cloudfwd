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
import com.splunk.cloudfwd.HecErrorResponseException;
import com.splunk.cloudfwd.http.lifecycle.LifecycleEvent;
import com.splunk.cloudfwd.http.lifecycle.RequestFailed;
import com.splunk.cloudfwd.http.lifecycle.Response;
import com.splunk.cloudfwd.http.lifecycle.EventBatchFailure;
import com.splunk.cloudfwd.http.lifecycle.EventBatchResponse;
import com.splunk.cloudfwd.http.lifecycle.EventBatchRequest;
import com.splunk.cloudfwd.util.PollScheduler;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.splunk.cloudfwd.http.lifecycle.PreRequest;
import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;

/**
 * HecIOManager is the mediator between sending and receiving messages to splunk
 * (as such it is the only piece of the Ack-system that touches the HttpSender).
 * HecIOManager sends via the sender and receives and unmarshals responses. From
 * these responses it maintains the ack window by adding newly received ackIds
 * to the ack window, or removing them on success. It also owns the
 * AckPollScheduler which will periodically call back "pollAcks" on this, which
 * sends the content of the ackTracker to Splunk via the sender, to check their
 * status.
 *
 * @author ghendrey
 */
public class HecIOManager implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(HecIOManager.class.getName());


  private static final ObjectMapper mapper = new ObjectMapper();
  private final HttpSender sender;
  private final PollScheduler ackPollController = new PollScheduler("ack poller");
  private final PollScheduler healthPollController = new PollScheduler(
          "health poller");
  private final AcknowledgementTracker ackTracker;
  private volatile boolean ackPollInProgress;

  HecIOManager(HttpSender sender) {
    this.sender = sender;
    this.ackTracker = new AcknowledgementTracker(sender);
  }

  /**
   * @return the ackPollReq
   */
  public String getAckPollReq() {
    return ackTracker.toString();
  }

  private synchronized void startPolling() {
    if (!ackPollController.isStarted()) {
      Runnable poller = () -> {
        if (this.getAcknowledgementTracker().isEmpty()) {
          LOG.trace("No acks to poll for");
          return;
        } else if (this.isAckPollInProgress()) {
          LOG.trace("skipping ack poll - already have one in flight");
          return;
        }
        this.pollAcks();
      };
      ackPollController.start(poller,
              sender.getConnection().getPropertiesFileHelper().getAckPollMS(),
              TimeUnit.MILLISECONDS);
    }
    if (!healthPollController.isStarted()) {
      Runnable poller = () -> {
        this.pollHealth();
      };
      healthPollController.start(
              poller, sender.getConnection().getPropertiesFileHelper().
              getHealthPollMS(), TimeUnit.MILLISECONDS);
    }
  }

  public void postEvents(EventBatch events) {
    //check to make sure the endpoint can absorb all the event formats in the batch
    events.checkCompatibility(sender.getConnection().getHecEndpointType());
    this.ackTracker.preEventPost(events);
    sender.getChannelMetrics().update(new EventBatchRequest(
            LifecycleEvent.Type.PRE_EVENT_POST, events));
    /*
    System.out.println(
            "channel=" + getSender().getChannel() + " events: " + this.
            toString());
     */

    FutureCallback<HttpResponse> cb = new AbstractHttpCallback() {

      @Override
      public void failed(Exception ex) {
        //eventPostFailure(ex);
        sender.getChannelMetrics().update(new EventBatchFailure(
                LifecycleEvent.Type.EVENT_POST_FAILURE, events, ex));
        sender.getConnection().getCallbacks().failed(events, ex);
      }

      @Override
      public void cancelled() {
        Exception ex = new RuntimeException(
                "HTTP post cancelled while posting events");
        sender.getConnection().getCallbacks().failed(events, ex);
      }

      @Override
      public void completed(String reply, int code) {
        if (code == 200) {
          try {
            consumeEventPostResponse(reply, events);
          } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            sender.getConnection().getCallbacks().failed(events, ex);
          }
        } else {
          sender.getChannelMetrics().update(new EventBatchResponse(
                  LifecycleEvent.Type.EVENT_POST_NOT_OK, code, reply,
                  events, sender.getBaseUrl()));
        }
      }
    };

    sender.postEvents(events, cb);
  }

  //called by AckMiddleware when event post response comes back with the indexer-generated ackId
  public void consumeEventPostResponse(String resp, EventBatch events) {
    //System.out.println("consuming event post response" + resp);
    EventPostResponseValueObject epr = null;

    try {
      Map<String, Object> map = mapper.readValue(resp,
              new TypeReference<Map<String, Object>>() {
      });
      epr = new EventPostResponseValueObject(map);
      events.setAckId(epr.getAckId()); //tell the batch what its HEC-generated ackId is.
    } catch (IllegalStateException e) {
      sender.getConnection().getCallbacks().failed(events,
              new HecErrorResponseException("ACK is disabled", 14, sender.getBaseUrl()));
    } catch (IOException ex) {
      LOG.error(ex.getMessage(), ex);
      throw new RuntimeException(ex.getMessage(), ex);
    }

    //System.out.println("ABOUT TO HANDLE EPR");
    ackTracker.handleEventPostResponse(epr, events);

    // start polling for acks
    startPolling();

    sender.getChannelMetrics().update(new EventBatchResponse(
            LifecycleEvent.Type.EVENT_POST_OK, 200, resp,
            events, sender.getBaseUrl()));
  }

  public void consumeAckPollResponse(String resp) {
    try {
      AckPollResponseValueObject ackPollResp = mapper.
              readValue(resp, AckPollResponseValueObject.class);
      this.ackTracker.handleAckPollResponse(ackPollResp);
    } catch (IOException ex) {
      throw new RuntimeException(ex.getMessage(), ex);
    }

  }

  public AcknowledgementTracker.AckRequest getAckPollRequest() {
    return ackTracker.getAckRequest();
  }

  public void setAckPollInProgress(boolean prog) {
    this.ackPollInProgress = prog;
  }

  //called by the AckPollScheduler
  public void pollAcks() {

    LOG.trace("POLLING ACKS...");
    sender.getChannelMetrics().update(new PreRequest(
            LifecycleEvent.Type.PRE_ACK_POLL));

    FutureCallback<HttpResponse> cb = new AbstractHttpCallback() {
      @Override
      public void completed(String reply, int code) {
        LOG.trace("channel: {} reply:{} ", HecIOManager.this.sender.getChannel(), reply);
        if (code == 200) {
          consumeAckPollResponse(reply);
        } else {
          sender.getChannelMetrics().update(new Response(
                  LifecycleEvent.Type.ACK_POLL_NOT_OK, code,
                  reply, sender.getBaseUrl()));
        }
        setAckPollInProgress(false);
      }

      @Override
      public void failed(Exception ex) {
        LOG.error("failed to poll acks: "+ex.getMessage(), ex);
        //AckManager.this.ackPollFailed(ex);
        sender.getChannelMetrics().update(new RequestFailed(
                LifecycleEvent.Type.ACK_POLL_FAILURE, ex));
        setAckPollInProgress(false);
      }

      @Override
      public void cancelled() {
        setAckPollInProgress(false);
        LOG.error("ack poll cancelled.");
      }
    };
    sender.pollAcks(this, cb);
  }

  private void setChannelHealth(int statusCode, String reply) {
    switch (statusCode) {
      case 200:
        LOG.info("Health check is good");
        sender.getChannelMetrics().update(new Response(
                LifecycleEvent.Type.HEALTH_POLL_OK,
                200, reply, sender.getBaseUrl()));
        break;
      case 503:
        sender.getChannelMetrics().update(new Response(
                LifecycleEvent.Type.HEALTH_POLL_INDEXER_BUSY,
                statusCode, reply, sender.getBaseUrl()));
        break;
      default:
        // Other status codes are not indicative of unhealthy HEC,
        // but rather the URL/token is wrong.
        // This is actually a failure.
        sender.getChannelMetrics().update(new Response(
                LifecycleEvent.Type.HEALTH_POLL_ERROR,
                statusCode, reply, sender.getBaseUrl()));
        break;
    }
  }

  public void pollHealth() {
    LOG.trace("polling health on {}...", sender.getChannel());

    FutureCallback<HttpResponse> cb = new AbstractHttpCallback() {
      @Override
      public void failed(Exception ex) {
        LOG.error("failed to poll health", ex);
        sender.getChannelMetrics().update(new RequestFailed(
                LifecycleEvent.Type.HEALTH_POLL_FAILED, ex));
      }

      @Override
      public void cancelled() {
        sender.getConnection().getCallbacks().failed(null, new Exception(
                "HEC health endpoint request cancelled."));
      }

      @Override
      public void completed(String reply, int code) {
        setChannelHealth(code, reply);
      }

    };
    sender.pollHealth(cb);
  }

  /**
   * Hits the /ack endpoint to check for a valid HEC token with acknowledgements
   * enabled
   */
  public void preFlightCheck() {
    LOG.info("PRE-FLIGHT CHECK...");

    FutureCallback<HttpResponse> cb = new AbstractHttpCallback() {
      @Override
      public void failed(Exception ex) {
        LOG.error("failed to perform pre-flight check", ex);
        sender.getChannelMetrics().update(new RequestFailed(
                LifecycleEvent.Type.PREFLIGHT_CHECK_FAILED, ex));
      }

      @Override
      public void cancelled() {
        sender.getConnection().getCallbacks().failed(null, new Exception(
                "HEC pre-flight check request cancelled."));
      }

      @Override
      public void completed(String reply, int code) {
        LifecycleEvent.Type type;
        if (code == 200) {
          LOG.info("PRE-FLIGHT CHECK OK");
          type = LifecycleEvent.Type.PREFLIGHT_CHECK_OK;
        } else {
          type = LifecycleEvent.Type.PREFLIGHT_CHECK_NOT_OK;
        }
        sender.getChannelMetrics().update(
            new Response(type, code, reply, sender.getBaseUrl()));
      }
    };

    sender.preFlightCheck(cb);
  }

  /**
   * @return the sender
   */
  HttpSender getSender() {
    return sender;
  }

  /**
   * @return the ackTracker
   */
  public AcknowledgementTracker getAcknowledgementTracker() {
    return ackTracker;
  }

  boolean isAckPollInProgress() {
    return this.ackPollInProgress;
  }

  @Override
  public void close() {
    this.ackPollController.stop();
    this.healthPollController.stop();
  }
}
