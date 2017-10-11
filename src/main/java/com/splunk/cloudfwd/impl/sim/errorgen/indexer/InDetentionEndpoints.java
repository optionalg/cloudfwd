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
package com.splunk.cloudfwd.impl.sim.errorgen.indexer;

import com.splunk.cloudfwd.impl.http.httpascync.HttpCallbacksAbstract;
import com.splunk.cloudfwd.impl.http.HecIOManager;
import com.splunk.cloudfwd.impl.http.HttpPostable;
import com.splunk.cloudfwd.impl.sim.AckEndpoint;
import com.splunk.cloudfwd.impl.sim.AcknowledgementEndpoint;
import com.splunk.cloudfwd.impl.sim.HealthEndpoint;
import com.splunk.cloudfwd.impl.sim.EventEndpoint;
import com.splunk.cloudfwd.impl.sim.SimulatedHECEndpoints;

import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;

/**
 * Simulate communicating with indexer in manual detention
 * @author meemax
 */
public class InDetentionEndpoints extends SimulatedHECEndpoints {
  @Override
  public void checkAckEndpoint(FutureCallback<HttpResponse> httpCallback) {
    ((HttpCallbacksAbstract)httpCallback).completed(
        "Not Found",
        404);
  }

  @Override
  protected AcknowledgementEndpoint createAckEndpoint() {
    return new InDetentionAckEndpoint();
  }

  @Override
  protected EventEndpoint createEventEndpoint() {
    return new InDetentionEventEndpoint();
  }

  @Override
  protected HealthEndpoint createHealthEndpoint() {
    return new InDetentionHealthEndpoint();
  }

  static class InDetentionAckEndpoint extends AckEndpoint {
    @Override
    public void pollAcks(HecIOManager ackMgr, FutureCallback<HttpResponse> cb) {
      System.out.println("/ack rest endpoint returns 404 on detention");
      ((HttpCallbacksAbstract) cb).completed(
              "Not Found",
              404);
    }

    @Override
    public void start() {

    }
  }

  static class InDetentionHealthEndpoint extends HealthEndpoint {
    @Override
    public void pollHealth(FutureCallback<HttpResponse> cb) {
      System.out.println("/health rest endpoint returns 404 on detention");
      ((HttpCallbacksAbstract) cb).completed(
              "Not Found",
              404);
    }
  }

  static class InDetentionEventEndpoint extends EventEndpoint {
    @Override
    public void post(HttpPostable events, FutureCallback<HttpResponse> cb) {
      System.out.println("/event rest endpoint returns 404 on detention");
      Runnable respond = () -> {
        ((HttpCallbacksAbstract) cb).completed(
            "Not Found",
            404);
      };
      delayResponse(respond);
    }
  }
}
