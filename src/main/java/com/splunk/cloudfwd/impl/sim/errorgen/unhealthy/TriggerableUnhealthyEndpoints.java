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

import com.splunk.cloudfwd.impl.ConnectionImpl;
import com.splunk.cloudfwd.impl.http.AbstractHttpCallback;
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
  private static final Logger LOG = ConnectionImpl.getLogger(TriggerableUnhealthyEndpoints.class.getName());

  public static boolean healthy = true;

  @Override
  protected HealthEndpoint createHealthEndpoint() {
    return new TriggerableHealthEndpoint();
  }

  private static class TriggerableHealthEndpoint extends HealthEndpoint {

    @Override
    public void pollHealth(FutureCallback<HttpResponse> cb) {
      if (healthy) {
        LOG.trace("HEALTH POLL OK");
        ((AbstractHttpCallback) cb).completed(               
                "If we care about the actual content, this will break something.",
                200);
      } else {
        LOG.trace("HEALTH POLL UNHEALTHY (503)");
        ((AbstractHttpCallback) cb).completed(
                "Simulated Indexer unhealthy queue is busy.",
                503);
      }

    }

  }
}
