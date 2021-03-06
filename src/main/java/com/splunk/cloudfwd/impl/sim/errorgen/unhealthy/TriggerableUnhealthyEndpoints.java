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
package com.splunk.cloudfwd.impl.sim.errorgen.unhealthy;

import com.splunk.cloudfwd.impl.http.HttpPostable;
import com.splunk.cloudfwd.impl.http.httpascync.HttpCallbacksAbstract;
import com.splunk.cloudfwd.impl.sim.AcknowledgementEndpoint;
import com.splunk.cloudfwd.impl.sim.EventEndpoint;
import com.splunk.cloudfwd.impl.sim.HealthEndpoint;
import com.splunk.cloudfwd.impl.sim.SimulatedHECEndpoints;
import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author ghendrey
 */
public class TriggerableUnhealthyEndpoints extends SimulatedHECEndpoints {
  private static final Logger LOG = LoggerFactory.getLogger(TriggerableUnhealthyEndpoints.class.getName());

  public static boolean healthy = true;
  
  @Override
  protected HealthEndpoint createHealthEndpoint() {
    return new TriggerableHealthEndpoint();
  }

  @Override
  protected EventEndpoint createEventEndpoint() {
    return new TriggerableEventEndpoint(ackEndpoint);
  }

  private static class TriggerableHealthEndpoint extends HealthEndpoint {

    @Override
    public void pollHealth(FutureCallback<HttpResponse> cb) {
      if (healthy) {
        LOG.trace("HEALTH POLL OK");
        ((HttpCallbacksAbstract) cb).completed(               
                "If we care about the actual content, this will break something.",
                200);
      } else {
        LOG.trace("HEALTH POLL UNHEALTHY (503)");
        ((HttpCallbacksAbstract) cb).completed(
                "Simulated Indexer unhealthy queue is busy.",
                503);
      }

    }

  }

  private static class TriggerableEventEndpoint extends EventEndpoint {
    public TriggerableEventEndpoint(AcknowledgementEndpoint ackEndpoint) {
      this.ackEndpoint = ackEndpoint;
      //ackEndpoint.start();
    }
    
    @Override
    public void post(HttpPostable events, FutureCallback<HttpResponse> cb) {
      Runnable respond;
      if (healthy) {
        respond = () -> {
          cb.completed(new EventPostResponse(
                  new AckIdRespEntity(nextAckId())
          ));
        };
      } else {
        respond = () -> {
          ((HttpCallbacksAbstract) cb).completed(
                  "Simulated Indexer unhealthy queue is busy.",
                  503);
        };
      }
      //return a single response with a delay uniformly distributed between  [0,5] ms
      delayResponse(respond);
    }
  }
}
