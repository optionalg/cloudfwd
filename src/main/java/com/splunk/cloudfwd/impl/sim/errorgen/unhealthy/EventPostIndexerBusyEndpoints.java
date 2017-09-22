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
import com.splunk.cloudfwd.impl.sim.EventEndpoint;
import com.splunk.cloudfwd.impl.sim.errorgen.HecErrorResponse;
import com.splunk.cloudfwd.impl.sim.CannedEntity;
import com.splunk.cloudfwd.impl.sim.SimulatedHECEndpoints;
import org.apache.http.HttpResponse;
import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.apache.http.concurrent.FutureCallback;

/**
 * Created by eprokop on 9/8/17.
 */
public class EventPostIndexerBusyEndpoints extends SimulatedHECEndpoints {
    @Override
    protected EventEndpoint createEventEndpoint() { return new IndexerBusyEventEndpoint(); }

    private static class IndexerBusyEventEndpoint extends EventEndpoint {
        @Override
        public void post(HttpPostable events, FutureCallback<HttpResponse> cb) {
            // In this scenario, assume that the indexer queue filled up AFTER our
            // last health or ack poll, and we are now trying to post events to a full indexer
            Runnable r = ()->cb.completed(new HecErrorResponse(
                        new IndexerBusyEntity(), new IndexerBusyStatusLine()));
            delayResponse(r);
        }
    }

    private static class IndexerBusyEntity extends CannedEntity {

        public IndexerBusyEntity() {
            super("{\"text\":\"Service Unavailable\",\"code\":9}");
        }
    }

    private static class IndexerBusyStatusLine implements StatusLine {
        @Override
        public ProtocolVersion getProtocolVersion() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public int getStatusCode() {
            return 503;
        }

        @Override
        public String getReasonPhrase() {
            throw new UnsupportedOperationException("Not supported yet.");
        }
    }

}
