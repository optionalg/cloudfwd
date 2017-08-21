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
package com.splunk.cloudfwd.sim.errorgen.unhealthy;

import com.splunk.cloudfwd.http.AbstractHttpCallback;
import com.splunk.cloudfwd.sim.HealthEndpoint;
import com.splunk.cloudfwd.sim.SimulatedHECEndpoints;
import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;

/**
 *
 * @author ghendrey
 */
public class TriggerableUnhealthyEndpoints extends SimulatedHECEndpoints {

  public static boolean healthy = true;

  @Override
  protected HealthEndpoint createHealthEndpoint() {
    return new TriggerableHealthEndpoint();
  }

  private static class TriggerableHealthEndpoint extends HealthEndpoint {

    @Override
    public void pollHealth(FutureCallback<HttpResponse> cb) {
      if (healthy) {
        System.out.println("HEALTH POLL OK");
        ((AbstractHttpCallback) cb).completed(               
                "If we care about the actual conent, this will break something.",
                200);
      } else {
        System.out.println("HEALTH POLL UNHEALTHY (503)");
        ((AbstractHttpCallback) cb).completed(
                "Simulated Inexer unhealthy queue is busy.",
                503);
      }

    }

  }
}
