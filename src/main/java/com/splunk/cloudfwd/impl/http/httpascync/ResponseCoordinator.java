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
import com.splunk.cloudfwd.impl.http.ChannelMetrics;
import com.splunk.cloudfwd.impl.http.lifecycle.Failure;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;

/**
 *
 * @author ghendrey
 */
public class ResponseCoordinator {

    private Logger LOG;
    //in the case of health polling, we hit both the ack and the health endpoint
    private final AtomicBoolean alreadyNotOk = new AtomicBoolean(false); //tells us if one of the two responses has already come back NOT OK
    private final AtomicInteger responseCount = new AtomicInteger(0); //count how many responses/fails we hsve processed
    private final int numExpectedResponses;
    private final LifecycleEventLatch[] latches ; //provide the ability to wait on any response in the serialized sequence

    private ResponseCoordinator(int numExpectedResponses) {
        this.numExpectedResponses = numExpectedResponses;
        latches = new LifecycleEventLatch[numExpectedResponses];
        for(int i=0;i<numExpectedResponses;i++){
            latches[i] = new LifecycleEventLatch(1);
        }
    }

    /**
     * Gets a TwoResponseCoodinator that can be used to coordinate two or more 
     * CoordinatedResponseHandlers
     *
     * @param serialize if set to true, awaitNthResponse() method will awaitNthResponse the arrival or
h1's response
     * @param responseHandlers each CoordinatedResponseHandler must be provided in the vararg list
     * @return
     */
    public static ResponseCoordinator create(CoordinatedResponseHandler... responseHandlers) {        
        ResponseCoordinator coord = new ResponseCoordinator(responseHandlers.length);
        for(CoordinatedResponseHandler h: responseHandlers){
            h.setCoordinator(coord);
        }
        coord.LOG = responseHandlers[0].getConnection().getLogger(ResponseCoordinator.class.
                getName());
        return coord;
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
     * @param channelMetrics will be updated if e is not ignorable
     */
    public synchronized void conditionallyUpate(LifecycleEvent e,
            ChannelMetrics channelMetrics) {
        LOG.trace("conditionallyUpdate for {}", e);
        latches[responseCount.get()].countDown(e); //allows someone to await the nth response 
        responseCount.incrementAndGet();
        if(!e.isOK()){
            alreadyNotOk.set(true); //record fact that we saw a not OK
        }
        //send OK response to /dev/null if they can be ignored
        if (isOKIgnorable(e)) {
            return; //under the above conditions we ignore OK responses
        }
        channelMetrics.update(e);    
    }
    
    public void cancel(LifecycleEvent e){        
        for(LifecycleEventLatch latch:latches){
            latch.countDown(e); //release anyone who might have been waiting
        }        
    }

    private boolean isOKIgnorable(LifecycleEvent e) {
        return okButNotLastResponse(e)
                || isOKButPredecessorWasNotOK(e);
    }

    private boolean isOKButPredecessorWasNotOK(LifecycleEvent e) {
        boolean ignore = alreadyNotOk.get() && e.isOK();
        if (ignore) {
            LOG.debug(
                    "Ignoring '{}' (already saw NOT OK on this channel)", e);
        }
        return ignore;
    }

    private boolean okButNotLastResponse(LifecycleEvent e) {
        boolean ignore = !isLast() && e.isOK();
        if (ignore) {
            LOG.debug(
                    "Ignoring OK response '{}' (This is not the last of {}).", e, latches.length);
        }
        return ignore;
    }

    private boolean isLast() {
        return (responseCount.get() == numExpectedResponses);
    }

    /**
     * Allows caller to seriaze request by awaiting a response before sending a request.
     * @param n the zero-based sequence number of the response you are waiting for.
     * @return the LifecycleEvent that corresponds to the nth response
     * @throws InterruptedException
     */
    public LifecycleEvent awaitNthResponse(int n) throws InterruptedException {
        //TODO FIXME - for sure we need to also have timeouts at the HTTP layer. If network is down this is going
        //to block the full five minutes. Furthermore, preflight retries will occur N times and the blocking will stack!
        if (latches[n].await(10, TimeUnit.SECONDS)) {//wait for ackcheck response before hitting ack endpoint  
            return this.latches[n].getLifecycleEvent();
        } else {
            LOG.warn("ResponseCoordinator timed out waiting 5 minutes for response {}.", n);
            return null;
        }
    }

    /**
     * @return the alreadyNotOk
     */
    public AtomicBoolean getAlreadyNotOk() {
        return alreadyNotOk;
    }

}
