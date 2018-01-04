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
package com.splunk.cloudfwd.impl.sim.errorgen.unknownhost;

import com.splunk.cloudfwd.impl.http.HecIOManager;
import com.splunk.cloudfwd.impl.http.HttpPostable;
import com.splunk.cloudfwd.impl.sim.SimulatedHECEndpoints;
import java.net.UnknownHostException;
import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;

/**
 *
 * @author ghendrey
 */
public class UnknownHostEndpoints extends SimulatedHECEndpoints{
    @Override
    public void postEvents(HttpPostable events,
            FutureCallback<HttpResponse> httpCallback) {
        httpCallback.failed(new UnknownHostException("Simulated UnknownHostException"));
    }

    @Override
    public void pollAcks(HecIOManager ackMgr,
            FutureCallback<HttpResponse> httpCallback) {
        httpCallback.failed(new UnknownHostException("Simulated UnknownHostException"));
    }

    @Override
    public void checkHealthEndpoint(FutureCallback<HttpResponse> httpCallback) {
        httpCallback.failed(new UnknownHostException("Simulated UnknownHostException"));
    }

    @Override
    public void checkRawEndpoint(
            FutureCallback<HttpResponse> httpCallback) {
      httpCallback.failed(new UnknownHostException("Simulated UnknownHostException"));
    }

    @Override
    public void checkAckEndpoint(FutureCallback<HttpResponse> httpCallback) {
        httpCallback.failed(new UnknownHostException("Simulated UnknownHostException"));
    }    
}
