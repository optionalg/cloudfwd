package com.splunk.cloudfwd.impl.sim.errorgen.unhealthy;

import com.splunk.cloudfwd.impl.http.HttpPostable;
import com.splunk.cloudfwd.impl.http.httpascync.HttpCallbacksAbstract;
import com.splunk.cloudfwd.impl.sim.EventEndpoint;
import com.splunk.cloudfwd.impl.sim.HealthEndpoint;
import com.splunk.cloudfwd.impl.sim.errorgen.HecErrorResponse;
import com.splunk.cloudfwd.impl.sim.CannedEntity;
import com.splunk.cloudfwd.impl.sim.SimulatedHECEndpoints;
import org.apache.http.HttpResponse;
import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.apache.http.concurrent.FutureCallback;

import java.util.concurrent.atomic.AtomicInteger;

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

/**
 * Created by eprokop on 9/29/17.
 */
public class EventPostGatewayTimeoutEndpoints extends SimulatedHECEndpoints {
    @Override
    protected EventEndpoint createEventEndpoint() { return new GatewayTimeoutEventEndpoint(); }
    
    @Override
    protected HealthEndpoint createHealthEndpoint() {
        return new GatewayTimeoutHealthEndpoint();
    }

    private static class GatewayTimeoutEventEndpoint extends EventEndpoint {
        @Override
        public void post(HttpPostable events, FutureCallback<HttpResponse> cb) {
            // In this scenario, assume that the indexer queue filled up AFTER our
            // last health or ack poll, and we are now trying to post events to a full indexer
            Runnable r = ()->cb.completed(new HecErrorResponse(
                    new GatewayTimeoutEntity(), new GatewayTimeoutStatusLine()));
            delayResponse(r);
        }
    }

    private static class GatewayTimeoutEntity extends CannedEntity {

        public GatewayTimeoutEntity() {
            super("N/A"); // all we care about is the HTTP code
        }
    }

    private static class GatewayTimeoutStatusLine implements StatusLine {
        @Override
        public ProtocolVersion getProtocolVersion() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public int getStatusCode() {
            return 504;
        }

        @Override
        public String getReasonPhrase() {
            throw new UnsupportedOperationException("Not supported yet.");
        }
    }
    
    private class GatewayTimeoutHealthEndpoint extends HealthEndpoint {
        private int numSuccesses = 2;
        private AtomicInteger i = new AtomicInteger(0);
        public void pollHealth(FutureCallback<HttpResponse> cb) {
            // preflight check must pass because we want connection instantiation to succeed. But afterwards, 
            // health polling should not mark the channels as healthy otherwise we get max retries exception on the resends
            // instead of the expected timeout exception. 
            if (i.get() < numSuccesses) {
                ((HttpCallbacksAbstract)cb).completed("N/A preflight OK", 200);
            } else {
                ((HttpCallbacksAbstract)cb).completed("N/A health poll gateway timeout", 504); 
            }
            i.incrementAndGet();
        }
    }
}
