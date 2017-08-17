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
package com.splunk.cloudfwd.sim.errorgen.nonsticky;

import com.splunk.cloudfwd.http.HecIOManager;
import com.splunk.cloudfwd.EventBatch;
import com.splunk.cloudfwd.sim.AckEndpoint;
import com.splunk.cloudfwd.sim.EventEndpoint;
import com.splunk.cloudfwd.sim.HealthEndpoint;
import com.splunk.cloudfwd.sim.SimulatedHECEndpoints;
import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;

/**
 *
 * @author ghendrey
 */
public class NonStickEndpoints extends SimulatedHECEndpoints {
  
  public NonStickEndpoints() {

  }
  
  
    @Override
  public void pollAcks(HecIOManager ackMgr,
          FutureCallback<HttpResponse> httpCallback) {
    //we have to ask the NonStickEventEndpoint for the AckEndpoint, because it switches between 
    //more than one AckEndpoint...THAT'S THE POINT of this simulator
    eventEndpoint.getAckEndpoint().pollAcks(ackMgr, httpCallback);
  }
  
  @Override
    protected AckEndpoint createAckEndpoint() {
    return null; //this class manages its own ack endpoints
  }

  @Override
  protected EventEndpoint createEventEndpoint() {
    return new NonStickEventEndpoint();
  }

  @Override
  protected HealthEndpoint createHealthEndpoint() {
    return new HealthEndpoint();
  }


  
}
