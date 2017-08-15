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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;

/**
 * AckManager is the mediator between sending and receiving messages to splunk
 * (as such it is the only piece of the Ack-system that touches the
 * HttpEventCollectorSender). AckManager sends via the sender and receives and
 * unmarshals responses. From these responses it maintains the ack window by
 * adding newly received ackIds to the ack window, or removing them on success.
 * It also owns the AckPollScheduler which will periodically call back
 * "pollAcks" on this, which sends the content of the ackWindow to Splunk via
 * the sender, to check their status.
 *
 * @author ghendrey
 */
public class AckManager implements AckLifecycle, Closeable {

  private static final Logger LOG = Logger.getLogger(AckManager.class.getName());

  private static final ObjectMapper mapper = new ObjectMapper();
  private final HttpEventCollectorSender sender;
  private final PollScheduler ackPollController;
  private final PollScheduler healthPollController;
  private final AckWindow ackWindow;
  private final ChannelMetrics channelMetrics;
  private volatile boolean ackPollInProgress;

  AckManager(HttpEventCollectorSender sender) {
    this.sender = sender;
    this.channelMetrics = new ChannelMetrics(sender);
    this.ackWindow = new AckWindow(sender, this.channelMetrics);
    this.ackPollController = new PollScheduler(sender.getConnection(), "ack poller");
    this.healthPollController = new PollScheduler(sender.getConnection(), "health poller");
    // start polling for health
  }

  /**
   * @return the ackPollReq
   */
  public String getAckPollReq() {
    return ackWindow.toString();
  }

  public ChannelMetrics getChannelMetrics() {
    return channelMetrics;
  }

  private synchronized void startPolling() {
    if (!ackPollController.isStarted()) {
      Runnable poller = () -> {
        if (this.getAckWindow().isEmpty()) {
          System.out.println("No acks to poll for");
          return;
        }else if (this.isAckPollInProgress()) {
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
    preEventsPost(events);
    /*
    System.out.println(
            "channel=" + getSender().getChannel() + " events: " + this.
            toString());
    */

    FutureCallback<HttpResponse> cb = new AbstractHttpCallback() {

      @Override
      public void failed(Exception e) {
        eventPostFailure(e);
        AckManager.this.sender.getConnection().getCallbacks().failed(events, e);
      }

      @Override
      public void cancelled() {
        Exception e = new RuntimeException("HTTP post cancelled while posting events");
        AckManager.this.sender.getConnection().getCallbacks().failed(events, e);
      }

      @Override
      public void completed(String reply, int code) {
        if (code == 200) {
          try {
            consumeEventPostResponse(reply, events);
          } catch (Exception e) {
            LOG.severe(e.getMessage());
            AckManager.this.sender.getConnection().getCallbacks().failed(events, e);
          }
        } else {
          String msg = "server didn't return ack ids";
          LOG.severe(msg);
          eventPostNotOK(code, reply, events);
          AckManager.this.sender.getConnection().getCallbacks().failed(events, new Exception(msg));
        }
      }

    };
    sender.postEvents(events, cb);

  }

  //called by AckMiddleware when event post response comes back with the indexer-generated ackId
  public void consumeEventPostResponse(String resp, EventBatch events) {
    //System.out.println("consuming event post response" + resp);
    EventPostResponse epr;
    try {
      Map<String, Object> map = mapper.readValue(resp,
              new TypeReference<Map<String, Object>>() {
      });
      epr = new EventPostResponse(map);
      events.setAckId(epr.getAckId()); //tell the batch what its HEC-generated ackId is.

      //System.out.println("ABOUT TO HANDLE EPR");
      ackWindow.handleEventPostResponse(epr, events);
    } catch (IOException e) {
      LOG.severe(e.getMessage());
      sender.getConnection().getCallbacks().failed(events, e);
    }

    // start polling for acks
    startPolling();
    
    eventPostOK(events);
  }
  

  public void consumeAckPollResponse(String resp) {
    try {
      AckPollResponse ackPollResp = mapper.
              readValue(resp, AckPollResponse.class);
      this.ackWindow.handleAckPollResponse(ackPollResp);
    } catch (IOException e) {
      LOG.severe(e.getMessage());
      sender.getConnection().getCallbacks().failed(null, e);
    }

  }

  //called by the AckPollScheduler
  public void pollAcks() {
    if (this.ackWindow.isEmpty()) {
      return; //ack poll scheduled but not needed
    }
    System.out.println("POLLING ACKS...");
    this.ackPollInProgress = true;
    preAckPoll();
    System.out.println("sending acks");
    FutureCallback<HttpResponse> cb = new AbstractHttpCallback() {
      @Override
      public void completed(String reply, int code) {
        System.out.println(
                "channel=" + AckManager.this.sender.getChannel() + " reply: " + reply);
        if (code == 200) {
          consumeAckPollResponse(reply);
        } else {
          ackPollNotOK(code, reply);
        }
        AckManager.this.ackPollInProgress = false;
      }

      @Override
      public void failed(Exception e) {
        String msg = "failed to poll acks";
        LOG.severe(msg);
        sender.getConnection().getCallbacks().failed(null, new Exception(msg + ": " + e.getMessage()));

        AckManager.this.ackPollFailed(e);
        AckManager.this.ackPollInProgress = false;
      }

      @Override
      public void cancelled() {
        String msg = "ack poll cancelled.";
        LOG.severe(msg);
        sender.getConnection().getCallbacks().failed(null, new Exception(msg));
      }
    };
    sender.pollAcks(this, cb);

  }

  public void setChannelHealth(int statusCode, String msg) {
    // For status code anything other 200
    switch (statusCode) {
      case 200:
        System.out.println("Health check is good");
        healthPollOK();
        break;
      case 503:
        healthPollNotOK(statusCode, msg);
        break;
      default:
        // 400 should not be indicative of unhealthy HEC
        // but rather the URL/token is wrong.
        healthPollFailed(new Exception(msg));
        break;
    }
  }

  public void pollHealth() {
    System.out.println("POLLING HEALTH...");

    FutureCallback<HttpResponse> cb = new AbstractHttpCallback() {
      @Override
      public void failed(Exception e) {
        String msg = "failed to poll health";
        LOG.severe(msg);
        healthPollFailed(e);
        sender.getConnection().getCallbacks().failed(null, new Exception(msg + ": " + e.getMessage()));
      }

      @Override
      public void cancelled() {
        String msg = "cancelled health poll";
        LOG.severe(msg);
        sender.getConnection().getCallbacks().failed(null, new Exception(msg));
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

  @Override
  public void preEventsPost(EventBatch events) {
    ackWindow.preEventPost(events);
    getChannelMetrics().preEventsPost(events);
  }

  @Override
  public void eventPostNotOK(int statusCode, String reply, EventBatch events) {
    getChannelMetrics().eventPostNotOK(statusCode, reply, events);
  }

  @Override
  public void preAckPoll() {
    getChannelMetrics().preAckPoll();
  }

  @Override
  public void eventPostOK(EventBatch events) {
      getChannelMetrics().eventPostOK(events);
  }

  @Override
  public void eventPostFailure(Exception ex) {    
    getChannelMetrics().eventPostFailure(ex);
  }

  @Override
  public void ackPollOK(EventBatch events) {
    //see consumeEventsPostResponse. We don't yet know what the events are that 
    //are correlated to the ack poll response! So this method can't ever be
    //legally called
    throw new IllegalStateException(
            "ackPollOK was illegally called on AckManager");
  }

  @Override
  public void ackPollNotOK(int statusCode, String reply) {
    String msg = "ack poll failed. Http status="+statusCode + ". Reply was " + reply;
    LOG.severe(msg);
    getChannelMetrics().ackPollNotOK(statusCode, reply);
    sender.getConnection().getCallbacks().failed(null, new Exception(msg));
  }

  @Override
  public void ackPollFailed(Exception e) {
    LOG.severe(e.getMessage());
    getChannelMetrics().ackPollFailed(e);
    sender.getConnection().getCallbacks().failed(null, e);
  }

  /**
   * @return the ackWindow
   */
  public AckWindow getAckWindow() {
    return ackWindow;
  }

  @Override
  public void healthPollFailed(Exception e) {
    LOG.severe(e.getMessage());
    getChannelMetrics().healthPollFailed(e);
    sender.getConnection().getCallbacks().failed(null, e);
  }

  @Override
  public void healthPollOK() {
    getChannelMetrics().healthPollOK();
  }

  @Override
  public void healthPollNotOK(int code, String msg) {
    getChannelMetrics().healthPollNotOK(code, msg);
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
