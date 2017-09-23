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

import com.splunk.cloudfwd.impl.ConnectionImpl;
import com.splunk.cloudfwd.impl.EventBatchImpl;
import com.splunk.cloudfwd.HecServerErrorResponseException;
import com.splunk.cloudfwd.impl.http.lifecycle.LifecycleEvent;
import com.splunk.cloudfwd.impl.http.lifecycle.RequestFailed;
import com.splunk.cloudfwd.impl.http.lifecycle.Response;
import com.splunk.cloudfwd.impl.http.lifecycle.EventBatchFailure;
import com.splunk.cloudfwd.impl.http.lifecycle.EventBatchResponse;
import com.splunk.cloudfwd.impl.http.lifecycle.EventBatchRequest;
import com.splunk.cloudfwd.impl.util.PollScheduler;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.splunk.cloudfwd.HecConnectionTimeoutException;
import com.splunk.cloudfwd.impl.http.lifecycle.PreRequest;
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
  private Logger LOG = LoggerFactory.getLogger(HecIOManager.class.getName());
  private static final ObjectMapper mapper = new ObjectMapper();
  private final HttpSender sender;
  private final PollScheduler ackPollController = new PollScheduler("ack poller");
  private final PollScheduler healthPollController = new PollScheduler("health poller");
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

  public void postEvents(EventBatchImpl events) {
    this.ackTracker.preEventPost(events);
    sender.getChannelMetrics().update(new EventBatchRequest(
            LifecycleEvent.Type.PRE_EVENT_POST, events));
    
    FutureCallback<HttpResponse> cb = new HecHttpEventPostCallbacks(sender.getConnection(), events);

    sender.postEvents(events, cb);
    sender.getChannelMetrics().update(new EventBatchRequest(
            LifecycleEvent.Type.EVENT_POSTED, events));
  }

  //called by AckMiddleware when event post response comes back with the indexer-generated ackId
  public void consumeEventPostResponse(String resp, EventBatchImpl events) throws HecServerErrorResponseException {
    //System.out.println("consuming event post response" + resp);
    EventPostResponseValueObject epr = null;

    try {
      Map<String, Object> map = mapper.readValue(resp,
              new TypeReference<Map<String, Object>>() {
      });
      if(!map.containsKey("ackId") || map.get("ackId") == null) {
        LOG.error("response {} lacks ackId field, but http code was 200. We infer that ack polling has been disabled.");
        sender.getChannelMetrics().update(new EventBatchResponse(
         LifecycleEvent.Type.ACK_POLL_DISABLED, 400, resp,
            events, sender.getBaseUrl()));          
         return; //we handled the non-normal event post response
      }
      epr = new EventPostResponseValueObject(map);
      events.setAckId(epr.getAckId()); //tell the batch what its HEC-generated ackId is.
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      return; 
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

    FutureCallback<HttpResponse> cb = new AbstractHttpCallback(sender.getConnection()) {
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
         LOG.error("Channel {} failed to poll acks", sender.getChannel(),  ex);
        LOG.error("failed to poll acks: "+ex.getMessage(), ex);
        //AckManager.this.ackPollFailed(ex);
        sender.getChannelMetrics().update(new RequestFailed(
                LifecycleEvent.Type.ACK_POLL_FAILURE, ex));
        setAckPollInProgress(false);
      }

      @Override
      public void cancelled() {
        LOG.error("Ack poll  cancelled on channel  {}", sender.getChannel());
        Exception ex = new RuntimeException(
                "HTTP post cancelled while polling for acks  on channel " + sender.getChannel());     
        sender.getChannelMetrics().update(new RequestFailed(
                LifecycleEvent.Type.ACK_POLL_FAILURE, ex));        
        setAckPollInProgress(false);

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
      case 404:
        sender.getChannelMetrics().update(new Response(
            LifecycleEvent.Type.SPLUNK_IN_DETENTION,
            statusCode, reply, sender.getBaseUrl()));
        break;
      case 400:
        // Other status codes are not indicative of unhealthy HEC,
        // but rather the URL/token is wrong.
        // This is actually a failure.
        sender.getChannelMetrics().update(new Response(
                LifecycleEvent.Type.HEALTH_POLL_ERROR,
                statusCode, reply, sender.getBaseUrl()));
        break;
      default:
        break;
    }
  }

  public void pollHealth() {
    LOG.trace("polling health on {}...", sender.getChannel());

    FutureCallback<HttpResponse> cb = new AbstractHttpCallback(sender.getConnection()) {
      @Override
      public void failed(Exception ex) {       
        LOG.error("Channel {} failed to poll health", sender.getChannel(),  ex);
        sender.getChannelMetrics().update(new RequestFailed(
                LifecycleEvent.Type.HEALTH_POLL_FAILED, ex));
      }

      @Override
      public void cancelled() {
        Exception ex = new RuntimeException(
                "HTTP post cancelled while polling for health on channel " + sender.getChannel());     
        sender.getChannelMetrics().update(new RequestFailed(
                LifecycleEvent.Type.HEALTH_POLL_FAILED, ex));             
      }

      @Override
      public void completed(String reply, int code) {
        setChannelHealth(code, reply);
      }

    };
    sender.pollHealth(cb);
  }
  
  // does not trigger a throw exception, called on a need-to-know
  // health check api
  private void setHecCheckHealth(int statusCode, String reply) {
    switch (statusCode) {
      case 200:
        LOG.info("HEC check is good");
        sender.getChannelMetrics().update(new Response(
                LifecycleEvent.Type.N2K_HEC_HEALTHY,
                200, reply, sender.getBaseUrl()));
        break;
      case 400:
        // determine code in reply, must be 14 for disabled
        ObjectMapper mapper = new ObjectMapper();
        HecErrorResponseValueObject hecErrorResp;
        try {
            hecErrorResp = mapper.readValue(reply,
                    HecErrorResponseValueObject.class);
        } catch (IOException ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
        if (14 == hecErrorResp.getCode()) {
          sender.getChannelMetrics().update(new Response(
              LifecycleEvent.Type.ACK_POLL_DISABLED,
              statusCode, reply, sender.getBaseUrl()));
        }
        break;
      case 404:
        sender.getChannelMetrics().update(new Response(
            LifecycleEvent.Type.SPLUNK_IN_DETENTION,
            statusCode, reply, sender.getBaseUrl()));
        break;
      case 403: //HTTPSTATUS_FORBIDDEN
        sender.getChannelMetrics().update(new Response(
                LifecycleEvent.Type.N2K_INVALID_TOKEN,
                statusCode, reply, sender.getBaseUrl()));
        break;
      case 401: //HTTPSTATUS_UNAUTHORIZED
        sender.getChannelMetrics().update(new Response(
                LifecycleEvent.Type.N2K_INVALID_AUTH,
                statusCode, reply, sender.getBaseUrl()));
        break;
      default:
        break;
    }
  }

  public void checkHealth() {
    LOG.trace("check health", sender.getChannel());

    FutureCallback<HttpResponse> cb = new AbstractHttpCallback(sender.getConnection()) {
      @Override
      public void failed(Exception ex) {
        LOG.error("HEC health check via /ack endpoint failed", ex);
        sender.getChannelMetrics().update(new RequestFailed(
                LifecycleEvent.Type.ACK_POLL_FAILURE, ex));
      }

      @Override
      public void cancelled() {
        sender.getConnection().getCallbacks().failed(null, new Exception(
                "HEC health check via /ack endpoint cancelled."));
      }

      @Override
      public void completed(String reply, int code) {
        setHecCheckHealth(code, reply);
      }

    };
    sender.splunkCheck(cb);
  }

  public void preflightCheck() {
    LOG.trace("preflight check health", sender.getChannel());

    FutureCallback<HttpResponse> cb = new AbstractHttpCallback(sender.getConnection()) {
      @Override
      public void failed(Exception ex) {
        LOG.error("HEC preflight health check via /ack endpoint failed", ex);
        sender.getChannelMetrics().update(new RequestFailed(
                LifecycleEvent.Type.PREFLIGHT_CHECK_FAILED, ex));
      }

      @Override
      public void cancelled() {
        sender.getConnection().getCallbacks().failed(null, new Exception(
                "HEC preflight health check via /ack endpoint cancelled."));
      }

      @Override
      public void completed(String reply, int code) {
        switch (code) {
        case 200:
          LOG.info("preflight check is good");
          sender.getChannelMetrics().update(new Response(
                  LifecycleEvent.Type.PREFLIGHT_CHECK_OK,
                  200, reply, sender.getBaseUrl()));
          break;
        default:
          sender.getChannelMetrics().update(new Response(
              LifecycleEvent.Type.PREFLIGHT_CHECK_ERROR,
              code, reply, sender.getBaseUrl()));
          break;
        }
      }
    };
    sender.splunkCheck(cb);
  }

  /**
   * @return the sender
   */
  public HttpSender getSender() {
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

  // Channel is now available, so can set Connection instance loggerFactory now
  public void setLogger(ConnectionImpl c) {
    this.LOG = c.getLogger(HecIOManager.class.getName());
    this.ackTracker.setLogger(c);
    this.ackPollController.setLogger(c);
    this.healthPollController.setLogger(c);
  }

    private class HecHttpEventPostCallbacks extends AbstractHttpCallback {

        private final EventBatchImpl events;

        public HecHttpEventPostCallbacks(ConnectionImpl connection,
                EventBatchImpl events) {
            super(connection);
            this.events = events;
        }

        @Override
        public void failed(Exception ex) {
            try{
                LOG.error("Event post failed on channel  {}", sender.getChannel(),  ex);
                LOG.error("Failed to post event batch {}", events,  ex);
                sender.getChannelMetrics().update(new EventBatchFailure(
                        LifecycleEvent.Type.EVENT_POST_FAILURE, events, ex));
                events.addSendException(ex);
                LOG.warn("resending events through load balancer {}", events);
                sender.getConnection().getLoadBalancer().sendRoundRobin(events, true);  //will callback failed if max retries exceeded
            }catch(Exception e){
                LOG.error(e.getMessage(), e);
                sender.getConnection().getCallbacks().failed(events, e);
            }
        }//end failed();

        @Override
        public void cancelled() {
            try{
                LOG.error("Event post cancelled on channel  {}, event batch {}", sender.getChannel(), events);
                Exception ex = new RuntimeException(
                        "HTTP post cancelled while posting events  "+events);
                sender.getChannelMetrics().update(new EventBatchFailure(
                        LifecycleEvent.Type.EVENT_POST_FAILURE, events, ex));
                sender.getConnection().getCallbacks().failed(events, ex);
            }catch(Exception e){
                LOG.error(e.getMessage(), e);
                sender.getConnection().getCallbacks().failed(events, e);
            }
        }//end cancelled()

        @Override
        public void completed(String reply, int code) {
            try {
                if (code == 200) {
                    consumeEventPostResponse(reply, events);
                } else {
                    handleNon200ServerResp(reply, code);
                }
            } catch (Exception ex) { //basically we should never see this
                LOG.error(ex.getMessage(), ex);
                sender.getConnection().getCallbacks().failed(events, ex);
            }
        }//end completed()

        private void handleNon200ServerResp(String reply, int code) throws HecConnectionTimeoutException, IOException {
            Map<String, Object> map;
            try{
                map  = mapper.readValue(reply,
                        new TypeReference<Map<String, Object>>() {
                        });
            }catch(Exception e){ //handle garbled/cut-off server response. We see this on ELB
                LOG.warn("Failed to read server response due to  {}", e);
                //we don't have "proof" the server is busy, but if response garbled we should act as though busy                
                markBusyAndResend(reply); 
                return;
            }
            if (!map.containsKey("ackId") || map.get("ackId") == null) {
                if (map.containsKey("code") && ((int) map.get("code")) == 9) {
                    LOG.warn("server busy: " + reply);
                    markBusyAndResend(reply);
                    return;
                }
            }
            //we don't know what the heck went wrong
            sender.getChannelMetrics().update(new EventBatchResponse(
                    LifecycleEvent.Type.EVENT_POST_NOT_OK, code, reply,
                    events, sender.getBaseUrl()));
        }//end handleNon200ServerResp

        private void markBusyAndResend(String reply) throws HecConnectionTimeoutException {
            //tell rest of the system "server busy". Not exactly true but will cause channel to get marked unhealthy
            sender.getChannelMetrics().update(new Response(
                    LifecycleEvent.Type.HEALTH_POLL_INDEXER_BUSY, //FIXME -- it's not really a "HEALTH_POLL". Prolly change this Type to be named just "INDEXER_BUSY"
                    9, reply, sender.getBaseUrl()));
            //resend
            LOG.warn("resending events through load balancer {}", events);
            sender.getConnection().getLoadBalancer().sendRoundRobin(events, true);  //will callback failed if max retries exceeded
        }
    }//end HecHttpCallbacks
}
