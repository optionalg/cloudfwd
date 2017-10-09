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

import com.splunk.cloudfwd.impl.http.httpascync.HttpCallbacksPreflightHealthCheck;
import com.splunk.cloudfwd.impl.http.httpascync.HttpCallbacksAckPoll;
import com.splunk.cloudfwd.impl.http.httpascync.HttpCallbacksEventPost;
import com.splunk.cloudfwd.impl.ConnectionImpl;
import com.splunk.cloudfwd.impl.EventBatchImpl;
import com.splunk.cloudfwd.impl.util.PollScheduler;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.splunk.cloudfwd.LifecycleEvent;
import com.splunk.cloudfwd.impl.http.httpascync.CoordinatedResponseHandler;
import com.splunk.cloudfwd.impl.http.httpascync.HttpCallbacksGeneric;
import com.splunk.cloudfwd.impl.http.httpascync.HttpCallbacksAbstract;
import com.splunk.cloudfwd.impl.http.httpascync.LifecycleEventLatch;
import java.io.Closeable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
    private final PollScheduler ackPollController = new PollScheduler(
            "ack poller");
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

    public synchronized void startAckPolling() {
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
                    sender.getConnection().getPropertiesFileHelper().
                    getAckPollMS(),
                    TimeUnit.MILLISECONDS);
        }
    }

    public void startHealthPolling() {
        if (!healthPollController.isStarted()) {
            healthPollController.start(
                    this::pollHealth, sender.getConnection().getPropertiesFileHelper().
                    getHealthPollMS(), TimeUnit.MILLISECONDS);
        }
    }

    public void postEvents(EventBatchImpl events) {
        this.ackTracker.preEventPost(events);
        FutureCallback<HttpResponse> cb = new HttpCallbacksEventPost(this, events);
        sender.postEvents(events, cb);
    }

    public AcknowledgementTracker.AckRequest getAckPollRequest() {
        return ackTracker.getAckRequest();
    }

    public void setAckPollInProgress(boolean prog) {
        this.ackPollInProgress = prog;
    }

    //called by the AckPollScheduler
    public void pollAcks() {
        LOG.trace("POLLING ACKS on {}", sender.getChannel());
        FutureCallback<HttpResponse> cb = new HttpCallbacksAckPoll(this);
        sender.pollAcks(this, cb);
    }

    public void pollHealth() {
        LOG.trace("health checks on {}", sender.getChannel());
        
         HttpCallbacksAbstract cb1 = new HttpCallbacksGeneric(this,
                LifecycleEvent.Type.HEALTH_POLL_OK,
                LifecycleEvent.Type.HEALTH_POLL_FAILED,
                "health_poll_health_endpoint_check");
        
        HttpCallbacksAbstract cb2 = new HttpCallbacksGeneric(this,
                LifecycleEvent.Type.HEALTH_POLL_OK,
                LifecycleEvent.Type.HEALTH_POLL_FAILED,
                "health_poll_ack_endpoint_check");
        
        trackTwoResponses(cb1, cb2);
        sender.pollHealth(cb1);
        sender.ackEndpointCheck(cb2);
    }

    //the two callback handlers must be made aware of each other's responses so that an OK response by one does not "hide"
    //a NOT OK response by the other.
    private void trackTwoResponses(HttpCallbacksAbstract cb1,
            HttpCallbacksAbstract cb2) {
        //A state tracker insures that if one of the two responses is NOT OK then
        //the HecChannel will get updated NOT OK, and not hidden by following OK response
        AtomicBoolean responseStateTracker = new AtomicBoolean(false);
        AtomicInteger respCount = new AtomicInteger(0);
        cb1.setTwoResponseStateTracker(responseStateTracker, respCount);
        cb2.setTwoResponseStateTracker(responseStateTracker, respCount);
    }


    public void preflightCheck() throws InterruptedException {
        LOG.trace("preflight checks on {}", sender.getChannel());
        AtomicBoolean responseStateTracker = new AtomicBoolean(false);
        AtomicInteger respCount = new AtomicInteger(0);
        CoordinatedResponseHandler cb1 = new CoordinatedResponseHandler(this, "preflight_ack_endpoint_check", respCount, responseStateTracker);
        HttpCallbacksAbstract cb2 = new HttpCallbacksPreflightHealthCheck(this, "preflight_health_endpoint_check");
        LifecycleEventLatch latch = new LifecycleEventLatch((1));
        trackTwoResponses(cb1, cb2);
        serializeRequests(latch, cb1, cb2);
        sender.ackEndpointCheck(cb1);
        try {
            //TODO FIXME - for sure we need to also have timeouts at the HTTP layer. If network is down this is going
            //to block the full five minutes. Furthermore, preflight retries will occur N times and the blocking will stack!
            if(latch.await(5, TimeUnit.MINUTES)){ //wait for ackcheck response before hitting ack endpoint  
                if(latch.getLifecycleEvent().isOK()){
                    sender.pollHealth(cb2); //we only proceed to check health endpoint if we got OK from ack check             
                }
            }else{
                LOG.warn("Preflight timed out (5 minutes)  waiting for ack endpoint check on {}", sender.getChannel());
               return; //FIXME -- can't we fail faster here? the latch will only be released on server response so we wait long time for failure
            }
        } catch (InterruptedException ex) {
            LOG.warn("Preflight interrupted on channel {} waiting for response from ack endpoint.", sender.getChannel());
            throw ex;
        }

    }
    
    private void serializeRequests(LifecycleEventLatch latch, HttpCallbacksAbstract cb1,
            HttpCallbacksAbstract cb2) {
        cb1.setLatch(latch);
        cb2.setLatch(latch);
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
}
