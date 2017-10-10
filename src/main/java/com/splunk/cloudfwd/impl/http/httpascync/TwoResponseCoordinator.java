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
package com.splunk.cloudfwd.impl.http.httpascync;

import com.splunk.cloudfwd.LifecycleEvent;
import com.splunk.cloudfwd.error.HecIllegalStateException;
import com.splunk.cloudfwd.impl.http.ChannelMetrics;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;

/**
 *
 * @author ghendrey
 */
public class TwoResponseCoordinator {

    private Logger LOG;
    //in the case of health polling, we hit both the ack and the health endpoint
    private AtomicBoolean alreadyNotOk = new AtomicBoolean(false); //tells us if one of the two responses has already come back NOT OK
    private AtomicInteger responseCount = new AtomicInteger(0); //count how many responses/fails we hsve processed
    //Due to ELB session cookies, preflight checks, which are the first requests sent on a channel, must be serialized if the req
    //is the first request sent over the channel. This allows the ELB Session-Cookie to take hold in the HTTP client.
    //This latch enables the serialization of requests.
    private LifecycleEventLatch latch = new LifecycleEventLatch(1); 
    private boolean serialize;

    private TwoResponseCoordinator() {

    }

    /**
     * Gets a TwoResponseCoodinator that can be used to coordinate two
     * CoordinatedResponseHandlers
     *
     * @param h1
     * @param h2
     * @param serialize if set to true, awaitFirstResponse() method will awaitFirstResponse the arrival or
 h1's response
     * @return
     */
    public static TwoResponseCoordinator create(CoordinatedResponseHandler h1,
            CoordinatedResponseHandler h2, boolean serialize) {
        TwoResponseCoordinator coord = new TwoResponseCoordinator();
        h1.setCoordinator(coord);
        h2.setCoordinator(coord);
        coord.setSerialize(serialize);
        coord.LOG = h1.getConnection().getLogger(TwoResponseCoordinator.class.
                getName());
        return coord;
    }

    public static TwoResponseCoordinator createNonSerializing(
            CoordinatedResponseHandler h1,
            CoordinatedResponseHandler h2) {
        return create(h1, h2, false);
    }

    public static TwoResponseCoordinator createSerializing(
            CoordinatedResponseHandler h1,
            CoordinatedResponseHandler h2) {
        return create(h1, h2, true);
    }

    /**
     * Two response coordinator tells us, through this call, if a given
     * lifecycle event should be ignored. An OK response must be ignored if it
     * is the first of two responses received. Otherwise it might make a channel
     * appear healthy when in fact we have not heard from the second
     * response....so we don't know for sure if channel is healthy. An OK
     * response must also be ignored if it *follows* a fail or NOT OK (otherwise
     * it would hide the NOT OK event).
     *
     * @param e the LifecycleEvent that we are asking if it should be ignored
     * @return true if the LifecycleEvent e should be igored
     */
    public synchronized void conditionallyUpate(LifecycleEvent e,
            ChannelMetrics m) {
        if (isSerialize()) {
            this.latch.countDown(e);
        }
        responseCount.incrementAndGet();
        alreadyNotOk.set(!e.isOK()); //record fact that we saw a not OK
        if (isFirstAndOK(e)
                || isOKButPredecessorWasNotOK(e)) {
            return; //under the above conditions we ignore OK responses
        }
        m.update(e);
    }

    private boolean isOKButPredecessorWasNotOK(LifecycleEvent e) {
        boolean ignore = alreadyNotOk.get() && e.isOK();
        if (ignore) {
            LOG.debug(
                    "Ignoring '{}' (already saw NOT OK on this channel)", e);
        }
        return ignore;
    }

    private boolean isFirstAndOK(LifecycleEvent e) {
        boolean ignore = isFirst() && e.isOK();
        if (ignore) {
            LOG.debug(
                    "Ignoring '{}' (must await second of 2 responses).", e);
        }
        return ignore;
    }

    private boolean isFirst() {
        return responseCount.get() == 1;
    }

    public LifecycleEvent awaitFirstResponse() throws InterruptedException {
        if (!isSerialize()) {
            throw new IllegalStateException(
                    "Cannot await non-serialized TwoResponseCoordinator");
        }
        //TODO FIXME - for sure we need to also have timeouts at the HTTP layer. If network is down this is going
        //to block the full five minutes. Furthermore, preflight retries will occur N times and the blocking will stack!
        if (latch.await(5, TimeUnit.MINUTES)) {//wait for ackcheck response before hitting ack endpoint  
            return this.latch.getLifecycleEvent();
        } else {
            LOG.warn("TwoResponseCoordinator timed out (5 minutes)  waiting for first response.");
            return null;
        }
    }

    /**
     * @return the alreadyNotOk
     */
    public AtomicBoolean getAlreadyNotOk() {
        return alreadyNotOk;
    }

    /**
     * @return the responseCount
     */
    public AtomicInteger getResponseCount() {
        return responseCount;
    }

    /**
     * @return the latch
     */
    public LifecycleEventLatch getLatch() {
        return latch;
    }

    /**
     * @return the serialize
     */
    public boolean isSerialize() {
        return serialize;
    }

    /**
     * @param serialize the serialize to set
     */
    public void setSerialize(boolean serialize) {
        this.serialize = serialize;
    }

}
