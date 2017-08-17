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
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;

/**
 * HecIOManager is the mediator between sending and receiving messages to splunk
 (as such it is the only piece of the Ack-system that touches the
 HttpEventCollectorSender). HecIOManager sends via the sender and receives and
 unmarshals responses. From these responses it maintains the ack window by
 adding newly received ackIds to the ack window, or removing them on success.
 It also owns the AckPollScheduler which will periodically call back
 "pollAcks" on this, which sends the content of the ackTracker to Splunk via
 the sender, to check their status.
 *
 * @author ghendrey
 */
public class HecIOManager implements Closeable {

  private static final Logger LOG = Logger.getLogger(HecIOManager.class.getName());

  private static final ObjectMapper mapper = new ObjectMapper();
  private final HttpEventCollectorSender sender;
  private final PollScheduler ackPollController = new PollScheduler("ack poller");
  private final PollScheduler healthPollController = new PollScheduler(
          "health poller");
  private final AcknowledgementTracker ackTracker;
  private volatile boolean ackPollInProgress;

  HecIOManager(HttpEventCollectorSender sender) {
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
          System.out.println("No acks to poll for");
          return;
        } else if (this.isAckPollInProgress()) {
          System.out.println("skipping ack poll - already have one in flight");
          return;
        }
        this.pollAcks();
      };
      ackPollController.start(poller, 250, TimeUnit.MILLISECONDS);
    }
    if (!healthPollController.isStarted()) {
      Runnable poller = () -> {
        this.pollHealth();
      };
      healthPollController.start(poller, 5, TimeUnit.SECONDS);
    }
  }

  public void postEvents(EventBatch events) {
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
            LOG.log(Level.SEVERE, null, ex);
          }
        } else {
          LOG.log(Level.SEVERE, "server didn't return ack ids");
          //eventPostNotOK(code, reply, events);
          sender.getChannelMetrics().update(new EventBatchResponse(
                  LifecycleEvent.Type.EVENT_POST_NOT_OK, code, reply, events));
        }
      }

    };
    sender.postEvents(events, cb);

  }

  //called by AckMiddleware when event post response comes back with the indexer-generated ackId
  public void consumeEventPostResponse(String resp, EventBatch events) {
    //System.out.println("consuming event post response" + resp);
    EventPostResponseValueObject epr;
    try {
      Map<String, Object> map = mapper.readValue(resp,
              new TypeReference<Map<String, Object>>() {
      });
      epr = new EventPostResponseValueObject(map);
      events.setAckId(epr.getAckId()); //tell the batch what its HEC-generated ackId is.
    } catch (IOException ex) {
      Logger.getLogger(getClass().getName()).log(Level.SEVERE, ex.getMessage(),
              ex);
      throw new RuntimeException(ex.getMessage(), ex);
    }

    //System.out.println("ABOUT TO HANDLE EPR");
    ackTracker.handleEventPostResponse(epr, events);

    // start polling for acks
    startPolling();

    sender.getChannelMetrics().update(new EventBatchResponse(
            LifecycleEvent.Type.EVENT_POST_OK, 200, resp, events));
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

  //called by the AckPollScheduler
  public void pollAcks() {
    if (this.ackTracker.isEmpty()) {
      return; //ack poll scheduled but not needed
    }
    System.out.println("POLLING ACKS...");
    this.ackPollInProgress = true;
    sender.getChannelMetrics().update(new PreRequest(LifecycleEvent.Type.PRE_ACK_POLL));
    System.out.println("sending acks");
    FutureCallback<HttpResponse> cb = new AbstractHttpCallback() {
      @Override
      public void completed(String reply, int code) {
        System.out.println("channel=" + HecIOManager.this.sender.getChannel() + " reply: " + reply);
        if (code == 200) {
          consumeAckPollResponse(reply);
        } else {
          sender.getChannelMetrics().update(new Response(
                  LifecycleEvent.Type.ACK_POLL_NOT_OK, code, reply));
        }
        HecIOManager.this.ackPollInProgress = false;
      }

      @Override
      public void failed(Exception ex) {
        LOG.log(Level.SEVERE, "failed to poll acks", ex);
        //AckManager.this.ackPollFailed(ex);
        sender.getChannelMetrics().update(new RequestFailed(
                LifecycleEvent.Type.ACK_POLL_FAILURE, ex));
        HecIOManager.this.ackPollInProgress = false;
      }

      @Override
      public void cancelled() {
        LOG.severe("ack poll cancelled.");
      }
    };
    sender.pollAcks(this, cb);

  }

  private void setChannelHealth(int statusCode, String msg) {
    // For status code anything other 200
    switch (statusCode) {
      case 200:
        System.out.println("Health check is good");
       sender.getChannelMetrics().update(new Response(LifecycleEvent.Type.HEALTH_POLL_OK,
                200, msg));
        break;
      case 503:
 
        sender.getChannelMetrics().update(new Response(
                LifecycleEvent.Type.HEALTH_POLL_NOT_OK, statusCode, msg));
        break;
      default:
        // 400 should not be indicative of unhealthy HEC
        // but rather the URL/token is wrong.
        //healthPollFailed(new Exception(msg));
        //this is actualy a failure. Something is actually *wrong* with the endpoint or configured URL
        sender.getConnection().getCallbacks().failed(null, new Exception(
                "HEC health endpoint returned " + statusCode + ". Response was: " + msg));
        break;
    }
  }

  public void pollHealth() {
    System.out.println("POLLING HEALTH...");

    FutureCallback<HttpResponse> cb = new AbstractHttpCallback() {
      @Override
      public void failed(Exception ex) {
        LOG.log(Level.SEVERE, "failed to poll health", ex);
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
   * @return the sender
   */
  HttpEventCollectorSender getSender() {
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
