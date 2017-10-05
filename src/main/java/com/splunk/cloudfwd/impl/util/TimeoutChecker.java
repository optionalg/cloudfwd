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
package com.splunk.cloudfwd.impl.util;

import com.splunk.cloudfwd.impl.ConnectionImpl;
import com.splunk.cloudfwd.impl.EventBatchImpl;
import com.splunk.cloudfwd.error.HecAcknowledgmentTimeoutException;
import static com.splunk.cloudfwd.LifecycleEvent.Type.EVENT_TIMED_OUT;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;

/**
 *
 * @author ghendrey
 */
public class TimeoutChecker implements EventTracker {
    private final Logger LOG;
    private PollScheduler timeoutCheckScheduler = new PollScheduler("Event Timeout Scheduler");
    private final Map<Comparable, EventBatchImpl> eventBatches = new ConcurrentHashMap<>();
    private ConnectionImpl connection;
    private boolean quiesced;

    public TimeoutChecker(ConnectionImpl c) {
        this.LOG = c.getLogger(TimeoutChecker.class.getName());
        timeoutCheckScheduler.setLogger(c);

        this.connection = c;
    }

    public void setTimeout(long ms) {
        queisce();
        start();
    }

    private long getTimeoutMs() {
        //check for timeouts with a minimum frequency of 1 second
        return connection.getPropertiesFileHelper().getAckTimeoutMS();
    }

    //how often we should rip through the list and check for timeouts
    private long getCheckInterval() {
        //minimum frequency, we check once per second. We can check more often, but never LESS oftern than that.
        return Math.min(getTimeoutMs(), 1000);
    }

    public synchronized void start() {
        timeoutCheckScheduler.start(this::checkTimeouts, getCheckInterval(),
                TimeUnit.MILLISECONDS);
    }

    private synchronized void checkTimeouts() {
        if (quiesced && eventBatches.isEmpty()) {
            LOG.debug("Stopping TimeoutChecker (no more unacked event batches)");
            //this is a one-off decoupling thread so that a thread owned by the timeoutCheckScheduler 
            //itself does not stop the timeoutCheckScheduler (which causes interrupted exception...
            //a thread running in a scheduler cannot awaitTermination of the scheduler itself WITHOUT
            //being interrupted. It's kinda like being asked to record your own time of death. Won't work.
            new Thread(()->{timeoutCheckScheduler.stop();},"TimeoutChecker closer").start();           
            return;
        }
        LOG.debug("checking timeouts for {} EventBatches", eventBatches.size());
        long now = System.currentTimeMillis();
        for (Iterator<Map.Entry<Comparable, EventBatchImpl>> iter = eventBatches.
                entrySet().
                iterator(); iter.hasNext();) {
            final Map.Entry<Comparable, EventBatchImpl> e = iter.next();
            EventBatchImpl events = e.getValue();
            if(events.isFailed()){
                iter.remove(); //ignore failed events
            }else if (events.isTimedOut(getTimeoutMs())) {
                events.setState(EVENT_TIMED_OUT);
                //this is the one case were we cannot call failed() directly, but rather have to go directly (via unwrap)
                //to the user-supplied callback. Otherwise we just loop back here over and over!
                ((CallbackInterceptor) connection.getCallbacks()).unwrap().
                        failed(events,
                                new HecAcknowledgmentTimeoutException(
                                        "EventBatch with id " + events.getId() + " timed out."));
                events.setFailed(true);
                iter.remove(); //remove it or else we will keep generating repeated timeout failures
            }
        }
    }

    public void queisce() {
        LOG.debug("Quiescing TimeoutChecker");
        quiesced = true;
        if (eventBatches.isEmpty()) {
            LOG.debug("Stopping TimeoutChecker (no EventBatches in flight)");
            timeoutCheckScheduler.stop();
        }
    }

    public void add(EventBatchImpl events) {
        this.eventBatches.put(events.getId(), events);
        events.registerEventTracker(this);
    }

    @Override
    public void cancel(EventBatchImpl events) {
        this.eventBatches.remove(events.getId());

    }

    public List<EventBatchImpl> getUnackedEvents(HecChannel c) {
        //return only the batches whose channel matches c
        return eventBatches.values().stream().filter(b -> {
            return b.getHecChannel().getChannelId() == c.getChannelId();
        }).collect(Collectors.toList());
    }

    public Collection<EventBatchImpl> getUnackedEvents() {
        return eventBatches.values();
    }

}
