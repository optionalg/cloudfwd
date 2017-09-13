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
package com.splunk.cloudfwd.util;

import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.EventBatch;
import com.splunk.cloudfwd.HecAcknowledgmentTimeoutException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author ghendrey
 */
public class TimeoutChecker implements EventTracker {

    protected static final Logger LOG = LoggerFactory.getLogger(
            TimeoutChecker.class.
            getName());

    private PollScheduler timoutCheckScheduler = new PollScheduler(
            "Event Timeout Scheduler");
    private final Map<Comparable, EventBatch> eventBatches = new ConcurrentHashMap<>();
    private Connection connection;
    private boolean quiesced;

    public TimeoutChecker(Connection c) {
        this.connection = c;
    }

    public void setTimeout(long ms) {
        queisce();
        start();
    }

    private long getTimeoutMs() {
        return connection.
                getPropertiesFileHelper().getAckTimeoutMS();
    }

    public synchronized void start() {
        timoutCheckScheduler.start(this::checkTimeouts, getTimeoutMs(),
                TimeUnit.MILLISECONDS);
    }

    private synchronized void checkTimeouts() {
        if(quiesced && eventBatches.isEmpty()){
            LOG.debug("Stopping TimeoutChecker (no more unacked event batches)");
            timoutCheckScheduler.stop();
            return;
        }
        LOG.debug("checking timeouts for {} EventBatches", eventBatches.size());
        long now = System.currentTimeMillis();
        for (Iterator<Map.Entry<Comparable, EventBatch>> iter = eventBatches.
                entrySet().
                iterator(); iter.hasNext();) {
            final Map.Entry<Comparable, EventBatch> e = iter.next();
            EventBatch events = e.getValue();
            if (events.isTimedOut(getTimeoutMs())) {
                //this is the one case were we cannot call failed() directly, but rather have to go directly (via unwrap)
                //to the user-supplied callback. Otherwise we just loop back here over and over!
                ((CallbackInterceptor) connection.getCallbacks()).unwrap().
                        failed(events,
                                new HecAcknowledgmentTimeoutException(
                                        "EventBatch with id " + events.getId() + " timed out."));
                iter.remove(); //remove it or else we will keep generating repeated timeout failures
            }
        }
    }

    public void queisce() {
        LOG.debug("Quiescing TimeoutChecker");
        quiesced = true;
        if(eventBatches.isEmpty()){
            LOG.debug("Stopping TimeoutChecker (no EventBatches in flight)");
            timoutCheckScheduler.stop();
        }
    }

    public void add(EventBatch events) {
        this.eventBatches.put(events.getId(), events);
        events.registerEventTracker(this);
    }

    @Override
    public void cancel(EventBatch events) {
        this.eventBatches.remove(events.getId());
    }

    public List<EventBatch> getUnackedEvents(HecChannel c) {
        //return only the batches whose channel matches c
        return eventBatches.values().stream().filter(b -> {
            return b.getHecChannel().getChannelId() == c.getChannelId();
        }).collect(Collectors.toList());
    }

}
