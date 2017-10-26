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
package com.splunk.cloudfwd.impl.sim.errorgen.cookies;

import com.splunk.cloudfwd.impl.http.HecIOManager;
import com.splunk.cloudfwd.impl.http.HttpPostable;
import com.splunk.cloudfwd.impl.http.httpascync.HttpCallbacksAbstract;
import com.splunk.cloudfwd.impl.sim.*;

import com.splunk.cloudfwd.impl.sim.errorgen.PreFlightAckEndpoint;
import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;

/*
Simulate endpoints with session cookies
 */

public class SimHecEndpointsWithCookies extends SimulatedHECEndpoints {

    @Override
    protected EventEndpoint createEventEndpoint() {
        return new SimHecEventEndpointWithCookies();
    }

    @Override
    protected HealthEndpoint createHealthEndpoint() {
        return new SimHecHealthEndpointWithCookies();
    }

    @Override
    protected AckEndpoint createAckEndpoint() {
        return new SimHecAckEndpointWithCookies();
    }

    @Override
    protected PreFlightAckEndpoint createPreFlightAckEndpoint() { return new SimPreFlightAckEndpointWithCookies();}
}

class SimHecEventEndpointWithCookies extends EventEndpoint {

    private HttpResponse response;

    @Override
    public void post(HttpPostable events, FutureCallback<HttpResponse> cb) {
        Runnable respond = () -> {
            ((HttpCallbacksAbstract) cb).completed(new CookieInjectedHttpResponse(response) {
            });
        };
        delayResponse(respond);
    }
}

class SimHecHealthEndpointWithCookies extends HealthEndpoint {

    private HttpResponse response;

    @Override
    public void pollHealth(FutureCallback<HttpResponse> cb) {
        Runnable respond = () -> {
            ((HttpCallbacksAbstract) cb).completed(new CookieInjectedHttpResponse(response) {
            });
        };
    }
}

class SimHecAckEndpointWithCookies extends AckEndpoint {

    private HttpResponse response;

    @Override
    public void pollAcks(HecIOManager mgr, FutureCallback<HttpResponse> cb) {
        Runnable respond = () -> {
            ((HttpCallbacksAbstract) cb).completed(new CookieInjectedHttpResponse(response) {
            });
        };
        delayResponse(respond);
    }

    @Override
    public void start() {
    }
}

class SimPreFlightAckEndpointWithCookies extends PreFlightAckEndpoint {

    private HttpResponse response;

    @Override
    public void checkAckEndpoint(FutureCallback<HttpResponse> cb) {
        Runnable respond = () -> {
            ((HttpCallbacksAbstract) cb).completed(new CookieInjectedHttpResponse(response) {
            });
        };

        delayResponse(respond);
    }
}
