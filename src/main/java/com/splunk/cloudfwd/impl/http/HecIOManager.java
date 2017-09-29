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
import com.splunk.cloudfwd.impl.http.httpascync.HttpCallbacksHealthPoll;
import com.splunk.cloudfwd.impl.http.httpascync.HttpCallbacksEventPost;
import com.splunk.cloudfwd.impl.ConnectionImpl;
import com.splunk.cloudfwd.impl.EventBatchImpl;
import com.splunk.cloudfwd.impl.util.PollScheduler;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.splunk.cloudfwd.impl.http.httpascync.HttpCallbacksBlockingConfigCheck;
import java.io.Closeable;
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

    public synchronized void startPolling() {
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
        LOG.trace("polling health on {}", sender.getChannel());
        FutureCallback<HttpResponse> cb = new HttpCallbacksHealthPoll(this);
        sender.pollHealth(cb);
    }


    public void preflightCheck() {
        LOG.trace("check health on {}", sender.getChannel());
        FutureCallback<HttpResponse> cb = new HttpCallbacksPreflightHealthCheck(this);
        sender.splunkCheck(cb);
    }
    
        public void configCheck(HttpCallbacksBlockingConfigCheck cb ) {
        LOG.trace("config check on {}", sender.getBaseUrl());
        sender.splunkCheck(cb);
        cb.setStarted(true);
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
