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
package com.splunk.cloudfwd.impl.sim.errorgen.slow;

import com.splunk.cloudfwd.impl.http.HecIOManager;
import com.splunk.cloudfwd.impl.sim.SimulatedHECEndpoints;
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
  public static long sleep = 1000; //1 second
  
  @Override
  public void pollAcks(HecIOManager ackMgr,
          FutureCallback<HttpResponse> httpCallback) {
    try {
      Thread.sleep(sleep);
    } catch (InterruptedException ex) {
        Thread.currentThread().interrupt(); //so we don't silently supporess interruption
        //we MUST resurn, not call ackEndpoint.pollAcks. Because this gets interrupted when the executor running the 
        //polling thread that calls this is shutdownNow. If you don't return then you fart out an ack poll response that 
        //*isn't* delayed by sleep, and MaxRetriesTest will fail
        return; //fixme todo this should probably throw the interrupted exception
    }
    ackEndpoint.pollAcks(ackMgr, httpCallback);
  }


}
