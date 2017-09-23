package com.splunk.cloudfwd.impl.sim.errorgen.unhealthy;

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

import com.splunk.cloudfwd.impl.http.HttpPostable;
import com.splunk.cloudfwd.impl.sim.CannedEntity;
import com.splunk.cloudfwd.impl.sim.EventEndpoint;
import com.splunk.cloudfwd.impl.sim.errorgen.HecErrorResponse;
import com.splunk.cloudfwd.impl.sim.SimulatedHECEndpoints;
import org.apache.http.HttpResponse;
import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.apache.http.concurrent.FutureCallback;

public class EventPostNoAckIdEndpoints extends SimulatedHECEndpoints {
    @Override
    protected EventEndpoint createEventEndpoint() { return new NoAckIdEndpoints(); }

    private static class NoAckIdEndpoints extends EventEndpoint {
        @Override
        public void post(HttpPostable events, FutureCallback<HttpResponse> cb) {
            // No Ack ID in the event response
            Runnable r = ()-> cb.completed(new HecErrorResponse(
                    new NoAckIdEntity(), new NoAckIdStatusLine()));
            delayResponse(r);
        }
    }

    private static class NoAckIdEntity extends CannedEntity {

        public NoAckIdEntity() {
            super("{\"text\":\"Success\",\"code\":0}}");
        }
    }

    private static class NoAckIdStatusLine implements StatusLine {
        @Override
        public ProtocolVersion getProtocolVersion() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public int getStatusCode() {
            return 200;
        }

        @Override
        public String getReasonPhrase() {
            throw new UnsupportedOperationException("Not supported yet.");
        }
    }
}
