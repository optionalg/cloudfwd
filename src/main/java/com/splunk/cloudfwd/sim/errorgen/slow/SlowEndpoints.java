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
package com.splunk.cloudfwd.sim.errorgen.slow;

import com.splunk.cloudfwd.http.AckManager;
import com.splunk.cloudfwd.sim.SimulatedHECEndpoints;
import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;

/**
 * Slow down the polling for acks. We do it simplistically by sleeping the call to pollAcks. Why not slow
 * down sendEvents()? Because that would block the thread executing the test. Then the test would take as 
 * long to finish as the slow endpoint is to return! We want the test to fail immediately when the Conncetion's
 * checks the event batch's isTimedOut method.
 * @author ghendrey
 */
public class SlowEndpoints extends SimulatedHECEndpoints {
  protected long sleep = 10000; //10 second

  @Override
  public void pollAcks(AckManager ackMgr,
          FutureCallback<HttpResponse> httpCallback) {
    sleep();
    ackEndpoint.pollAcks(ackMgr, httpCallback);
  }

  private void sleep() {
    try {
      Thread.sleep(sleep);
    } catch (InterruptedException ex) {
      throw new RuntimeException(ex.getMessage(), ex);
    }
  }
}
