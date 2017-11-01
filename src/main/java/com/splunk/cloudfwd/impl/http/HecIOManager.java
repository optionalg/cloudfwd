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

import com.splunk.cloudfwd.impl.http.httpascync.HttpCallbacksAckPoll;
import com.splunk.cloudfwd.impl.http.httpascync.HttpCallbacksEventPost;
import com.splunk.cloudfwd.impl.ConnectionImpl;
import com.splunk.cloudfwd.impl.EventBatchImpl;
import com.splunk.cloudfwd.impl.util.ThreadScheduler;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.splunk.cloudfwd.LifecycleEvent;
import com.splunk.cloudfwd.impl.http.httpascync.GenericCoordinatedResponseHandler;
import com.splunk.cloudfwd.impl.http.httpascync.NoDataEventPostResponseHandler;
import com.splunk.cloudfwd.impl.http.httpascync.ResponseCoordinator;
import java.io.Closeable;
import java.util.concurrent.ScheduledFuture;
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
//    private final ThreadScheduler ackPollController = new ThreadScheduler(
//            "ack poller");
//    private final ThreadScheduler healthPollController = new ThreadScheduler(
//            "health poller");
    private volatile ScheduledFuture ackPollTask;
    private volatile ScheduledFuture healthPollTask;
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

    public void startAckPolling() {
        if(null != ackPollTask){
            return;
        }
        synchronized(this){
            if (null == ackPollTask) {
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
                long interval = sender.getConnection().getSettings().getAckPollMS();
                this.ackPollTask = ThreadScheduler.getSchedulerInstance("ack poller").scheduleWithFixedDelay(poller, (long) (interval*Math.random()), interval, TimeUnit.MILLISECONDS);
            }
        }
    }

    public void startHealthPolling() {
        if(null != healthPollTask){
            return;
        }
        synchronized(this){
            if (null == healthPollTask) {
                long interval = sender.getConnection().getSettings().
                        getHealthPollMS();
                this.healthPollTask = ThreadScheduler.getSchedulerInstance("health poller").scheduleWithFixedDelay(this::pollHealth, (long) (interval*Math.random()), interval, TimeUnit.MILLISECONDS);
            }
        }
    }

    public void postEvents(EventBatchImpl events) {
        this.ackTracker.preEventPost(events);
        FutureCallback<HttpResponse> cb = new HttpCallbacksEventPost(this,
                events);
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
//        ThreadScheduler.getExecutorInstance("ack_poll_executor_thread").execute(
//                ()->{
                     LOG.trace("POLLING ACKS on {}", sender.getChannel());
                    FutureCallback<HttpResponse> cb = new HttpCallbacksAckPoll(this);
                    sender.pollAcks(this, cb);
             //   });
    }

    /**
     * This will get invoked after the HecChannel completes preflight checks
     * successfully
     */
    public void pollHealth() {
        //ThreadScheduler.getExecutorInstance("health_poll_executor_thread").execute(
            //    ()->{
                    LOG.trace("health checks on {}", sender.getChannel());

                    GenericCoordinatedResponseHandler cb1 = new GenericCoordinatedResponseHandler(
                            this,
                            LifecycleEvent.Type.HEALTH_POLL_OK,
                            LifecycleEvent.Type.HEALTH_POLL_FAILED,
                            "health_poll_health_endpoint_check");

                    GenericCoordinatedResponseHandler cb2 = new GenericCoordinatedResponseHandler(
                            this,
                            LifecycleEvent.Type.HEALTH_POLL_OK,
                            LifecycleEvent.Type.HEALTH_POLL_FAILED,
                            "health_poll_ack_endpoint_check");

                    GenericCoordinatedResponseHandler cb3 = new GenericCoordinatedResponseHandler(
                            this,
                            LifecycleEvent.Type.HEALTH_POLL_OK,
                            LifecycleEvent.Type.HEALTH_POLL_FAILED,
                            "health_poll_raw_endpoint_check");

                    ResponseCoordinator.create(cb1, cb2, cb3);
                    sender.checkHealthEndpoint(cb1);
                    sender.checkAckEndpoint(cb2);
                    sender.checkAckEndpoint(cb3);
       // });
    }

    public void preflightCheck() {
//        ThreadScheduler.getExecutorInstance("preflight_executor_thread").execute(
//                ()->{      
        try {
            LOG.trace("preflight checks on {}", sender.getChannel());
            GenericCoordinatedResponseHandler cb1 = new GenericCoordinatedResponseHandler(
                    this,
                    LifecycleEvent.Type.PREFLIGHT_OK,
                    LifecycleEvent.Type.PREFLIGHT_FAILED,
                    LifecycleEvent.Type.PREFLIGHT_GATEWAY_TIMEOUT,
                    LifecycleEvent.Type.PREFLIGHT_BUSY,
                    "preflight_ack_endpoint_check");
            GenericCoordinatedResponseHandler cb2 = new GenericCoordinatedResponseHandler(
                    this,
                    LifecycleEvent.Type.PREFLIGHT_OK,
                    LifecycleEvent.Type.PREFLIGHT_FAILED,
                    LifecycleEvent.Type.PREFLIGHT_GATEWAY_TIMEOUT,
                    LifecycleEvent.Type.PREFLIGHT_BUSY,
                    "preflight_health_endpoint_check");
            GenericCoordinatedResponseHandler cb3 = new NoDataEventPostResponseHandler(
                    this,
                    LifecycleEvent.Type.PREFLIGHT_OK,
                    LifecycleEvent.Type.PREFLIGHT_FAILED,
                    LifecycleEvent.Type.PREFLIGHT_GATEWAY_TIMEOUT,
                    LifecycleEvent.Type.PREFLIGHT_BUSY,
                    "preflight_raw_endpoint_check");
            ResponseCoordinator coordinator = ResponseCoordinator.create(cb1,
                    cb2,
                    cb3);
            sender.checkAckEndpoint(cb1);//SEND FIRST REQUEST

            LifecycleEvent firstResp = coordinator.awaitNthResponse(0); //WAIT FIRST RESPONSE
            if (null != firstResp) {
                if (firstResp.isOK()) {
                    sender.checkHealthEndpoint(cb2); //SEND SECOND REQUEST 
                    LifecycleEvent secondResp = coordinator.awaitNthResponse(1); //WAIT SECOND RESPONSE
                    if (null != secondResp) {
                        if (secondResp.isOK()) {
                            sender.checkRawEndpoint(cb3); //SEND THIRD REQUEST
                        }
                    } else {
                        LOG.warn(
                                "Preflight timed out (5 minutes)  waiting for /raw empty-event check on {}",
                                sender.getChannel());
                    }
                } else {
                    LOG.warn(
                            "Preflight timed out (5 minutes)  waiting for /health check on {}",
                            sender.getChannel());
                }
            } else {
                LOG.warn(
                        "Preflight timed out (5 minutes)  waiting for /ack endpoint check on {}",
                        sender.getChannel());
            }
        } catch (InterruptedException ex) {
            LOG.warn(
                    "Preflight interrupted on channel {} waiting for response from ack endpoint.",
                    sender.getChannel());
            //throw ex;
        }catch (Exception ex) {
            LOG.error("{}", ex.getMessage(), ex);
            //throw ex;
        }
        
//        });

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
        if(null != ackPollTask && !ackPollTask.isCancelled()){
            this.ackPollTask.cancel(true);
        }
        if(null != healthPollTask && !healthPollTask.isCancelled()){
            this.healthPollTask.cancel(true);
        }
    }

    // Channel is now available, so can set Connection instance loggerFactory now
    public void setLogger(ConnectionImpl c) {
        this.LOG = c.getLogger(HecIOManager.class.getName());
        this.ackTracker.setLogger(c);
    }
}
