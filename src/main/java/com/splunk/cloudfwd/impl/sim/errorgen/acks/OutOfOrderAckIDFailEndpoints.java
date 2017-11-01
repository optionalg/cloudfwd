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
package com.splunk.cloudfwd.impl.sim.errorgen.acks;

import com.splunk.cloudfwd.impl.http.HecIOManager;
import com.splunk.cloudfwd.impl.http.HttpPostable;
import com.splunk.cloudfwd.impl.http.httpascync.HttpCallbacksAbstract;
import com.splunk.cloudfwd.impl.sim.AcknowledgementEndpoint;
import com.splunk.cloudfwd.impl.sim.RandomAckEndpoint;
import com.splunk.cloudfwd.impl.sim.SimulatedHECEndpoints;

import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author meemax
 */
public class OutOfOrderAckIDFailEndpoints extends SimulatedHECEndpoints {
  private static final Logger LOG = LoggerFactory.getLogger(OutOfOrderAckIDFailEndpoints.class.getName());
  private static boolean fail = false;
  
  public static void toggleFail(boolean forceFail) {
    fail = forceFail;
  }
/*  
  @Override
  public void pollAcks(HecIOManager ackMgr,
          FutureCallback<HttpResponse> httpCallback) {
    if (fail) {
      System.out.println("/health rest endpoint returns 404 on detention");
      ((HttpCallbacksAbstract)httpCallback).completed(
              "Not Found",
              404);
    }
    else {
      this.ackEndpoint.pollAcks(ackMgr, httpCallback);
    }
  }

  @Override
  public void checkHealthEndpoint(FutureCallback<HttpResponse> httpCallback) {
    if (fail) {
      System.out.println("/health rest endpoint returns 404 on detention");
      ((HttpCallbacksAbstract) httpCallback).completed(
              "Not Found",
              404);
    }
    else {
      this.healthEndpoint.pollHealth(httpCallback);
    }
  }
*/
  @Override
  public void postEvents(HttpPostable events,
          FutureCallback<HttpResponse> httpCallback) {
    if (fail) {
      System.out.println("/event rest endpoint returns 404 on detention");
      ((HttpCallbacksAbstract) httpCallback).completed(
              "Not Found",
              404);
    }
    else {
      this.eventEndpoint.post(events, httpCallback);
    }
  }

  @Override
  protected AcknowledgementEndpoint createAckEndpoint() {
    return new RandomAckEndpoint();
  }
}
