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
package com.splunk.cloudfwd.impl.sim.errorgen.invalidvent;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.splunk.cloudfwd.impl.http.HecErrorResponseValueObject;
import com.splunk.cloudfwd.impl.http.HttpPostable;
import com.splunk.cloudfwd.impl.http.httpascync.HttpCallbacksAbstract;
import com.splunk.cloudfwd.impl.sim.EventEndpoint;
import com.splunk.cloudfwd.impl.sim.SimulatedHECEndpoints;
import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;

/**
 *returns a 400 server reply: {"text":"Invalid data format","code":6,"invalid-event-number":0}
 * @author ghendrey
 */
public class InvalidEventEndpoint extends SimulatedHECEndpoints {

    @Override
    protected EventEndpoint createEventEndpoint() {
        final HecErrorResponseValueObject vo = new HecErrorResponseValueObject("Invalid data format", 6, 0);
        return new EventEndpoint() {
            public void post(HttpPostable events,
                    FutureCallback<HttpResponse> cb) {
                Runnable respond = () -> {
                    try {
                        ((HttpCallbacksAbstract)cb).completed(vo.toJson(),400);
                    } catch (JsonProcessingException ex) {
                        throw new RuntimeException(ex.getMessage(), ex);
                    }
                };
                //return a single response with a delay uniformly distributed between  [0,5] ms
                delayResponse(respond);
            }

        };
    }
}
