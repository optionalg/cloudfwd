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

import com.splunk.cloudfwd.http.AckManager;
import com.splunk.cloudfwd.http.EventBatch;
import com.splunk.cloudfwd.sim.HealthEndpoint;
import com.splunk.cloudfwd.sim.SimulatedHECEndpoints;
import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;

/**
 *
 * @author ghendrey
 */
public class Endpoints extends SimulatedHECEndpoints {
  
  public Endpoints() {
    super.eventEndpoint= new NonStickEventEndpoint(); //will set itself up with several channels
    super.healthEndpoint = new HealthEndpoint();
    super.ackEndpoint = null; //not used (instead we let the NonStickEventsEndpoint manage the ackEndpoint)
  }
  
  @Override
  public void postEvents(EventBatch events,
          FutureCallback<HttpResponse> httpCallback) {
    eventEndpoint.post(events, httpCallback);
  }
  
    @Override
  public void pollAcks(AckManager ackMgr,
          FutureCallback<HttpResponse> httpCallback) {
    //we have to ask the NonStickEventEndpoint for the AckEndpoint, because it switches between 
    //more than one AckEndpoint...THAT'S THE POINT of this simulator
    eventEndpoint.getAckEndpoint().pollAcks(ackMgr, httpCallback);
  }

    @Override
  public void close()  {
    ((NonStickEventEndpoint)eventEndpoint).close();
  }
  
}
