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
import com.splunk.cloudfwd.impl.http.HecIOManager;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;

/**
 *
 * @author ghendrey
 */
public abstract class CoordinatedResponseHandler extends HttpCallbacksAbstract{

    private Logger LOG;
    //in the case of health polling, we hit both the ack and the health endpoint
    private AtomicBoolean alreadyNotOk; //tells us if one of the two responses has already come back NOT OK
    private AtomicInteger responseCount;
    //Due to ELB session cookies, preflight checks, which are the first requests sent on a channel, must be serialized.
    //This latch enables the serialization of requests.
    private LifecycleEventLatch latch; //provided by external source who wants to serialize two *requests*

    public CoordinatedResponseHandler(HecIOManager m, LifecycleEvent.Type okType,
            LifecycleEvent.Type failType, String name, AtomicBoolean alreadyNotOk, AtomicInteger respCount) {
        super(m);
        this.alreadyNotOk = alreadyNotOk;
        this.responseCount = respCount;        
        this.LOG = m.getSender().getConnection().getLogger(
                HttpCallbacksGeneric.class.
                getName());
    }

    public void setLatch(LifecycleEventLatch latch) {
        this.latch = latch;
    }

    @Override
    protected void notify(LifecycleEvent e) {
        synchronized (alreadyNotOk) {
            if (null != latch) {
                latch.countDown(e);//got a response or failed. Preflight can now look at e and decide how to proceed
            }
            responseCount.incrementAndGet();
            if (alreadyNotOk.get()) {
                if (e.isOK()) {
                    LOG.debug(
                            "Ignoring OK response from '{}' because already got not ok from other endpoint on '{}'.",
                            getName(), getChannel());
                    return; //must not update status to OK, when the other of two expected response isn't NOT OK.
                }
            }
            if (responseCount.get() == 1 && e.isOK()) {
                LOG.debug(
                        "Ignoring FIRST OK response from '{}' because waiting for other resp on '{}'.",
                        getName(), getChannel());
                return; //discard the first response if it is OK. We have to wait for the 2nd, which will be authoratitive
            }
            alreadyNotOk.set(!e.isOK()); //record fact that we saw a not OK
            //the update must occur in the synchronized block when we are latched on two requests   
            getSender().getChannelMetrics().update(e);
            return;
        }
    }

}
