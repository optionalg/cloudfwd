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
package com.splunk.cloudfwd.impl.sim;

import com.splunk.cloudfwd.impl.http.HecIOManager;
import com.splunk.cloudfwd.impl.http.Endpoints;
import com.splunk.cloudfwd.impl.http.HttpPostable;
import com.splunk.cloudfwd.impl.sim.errorgen.PreFlightAckEndpoint;
import com.splunk.cloudfwd.impl.sim.errorgen.HecErrorResponse;
import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author ghendrey
 */
public class SimulatedHECEndpoints implements Endpoints {
    private static final Logger LOG = LoggerFactory.getLogger(SimulatedHECEndpoints.class.getName());
    
    protected AcknowledgementEndpoint ackEndpoint;
    protected EventEndpoint eventEndpoint;
    protected HealthEndpoint healthEndpoint;
    protected PreFlightAckEndpoint preFlightAckEndpoint;
    protected boolean started;

    @Override
    public void postEvents(HttpPostable events,
            FutureCallback<HttpResponse> httpCallback) {
        eventEndpoint.post(events, httpCallback);
    }

    @Override
    public void pollAcks(HecIOManager ackMgr,
            FutureCallback<HttpResponse> httpCallback) {
        ackEndpoint.pollAcks(ackMgr, httpCallback);
    }

    @Override
    public void checkHealthEndpoint(FutureCallback<HttpResponse> httpCallback) {
        this.healthEndpoint.pollHealth(httpCallback);
    }

    @Override
    public void checkRawEndpoint(
            FutureCallback<HttpResponse> httpCallback) {
        httpCallback.completed(new HecErrorResponse(
                new TokenEnabledEntity(), new BadRequestStatusLine()
        ));
    }

    private static class TokenEnabledEntity extends CannedEntity {
        public TokenEnabledEntity() {
            super("{\"text\":\"No data\",\"code\":5}");
        }
    }

    @Override
    public void checkAckEndpoint(FutureCallback<HttpResponse> httpCallback) {
        preFlightAckEndpoint.checkAckEndpoint(httpCallback);
    }

    @Override
    public final void close() {
        if (null != ackEndpoint) {
            ackEndpoint.close();
        }
        if (null != eventEndpoint) {
            eventEndpoint.close();
        }
        if (null != healthEndpoint) {
            healthEndpoint.close();
        }
        if(null != preFlightAckEndpoint) {
            preFlightAckEndpoint.close();
        }
    }

    @Override
    public synchronized void start() {
        if (started) {
            return;
        }
        this.ackEndpoint = createAckEndpoint();
        if (null != ackEndpoint) {
            ackEndpoint.start();
        }
        this.eventEndpoint = createEventEndpoint();
        if (null != eventEndpoint) {
            eventEndpoint.start();
        }
        this.healthEndpoint = createHealthEndpoint();
        if (null != healthEndpoint) {
            healthEndpoint.start();
        }
        this.preFlightAckEndpoint = createPreFlightAckEndpoint();
        if (null != preFlightAckEndpoint) {
            preFlightAckEndpoint.start();
        }
        started = true;
    }

    protected AcknowledgementEndpoint createAckEndpoint() {
        return new AckEndpoint();
    }

    protected EventEndpoint createEventEndpoint() {
        return new EventEndpoint(ackEndpoint);
    }

    protected HealthEndpoint createHealthEndpoint() {
        return new HealthEndpoint();
    }

    protected PreFlightAckEndpoint createPreFlightAckEndpoint() {
        return new PreFlightAckEndpoint();
    }

}
