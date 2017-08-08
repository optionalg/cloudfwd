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
package com.splunk.cloudfwd.sim;

import com.splunk.cloudfwd.http.AckManager;
import com.splunk.cloudfwd.http.Endpoints;
import com.splunk.cloudfwd.http.EventBatch;
import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;

/**
 *
 * @author ghendrey
 */
public class SimulatedHECEndpoints implements Endpoints{
  protected AckEndpoint ackEndpoint;
  protected EventEndpoint eventEndpoint;
  protected HealthEndpoint healthEndpoint;
  private boolean started;
  

  @Override
  public void postEvents(EventBatch events,
          FutureCallback<HttpResponse> httpCallback) {
    eventEndpoint.post(events, httpCallback);
  }

  @Override
  public void pollAcks(AckManager ackMgr,
          FutureCallback<HttpResponse> httpCallback) {
    ackEndpoint.pollAcks(ackMgr, httpCallback);
  }

  @Override
  public void pollHealth(FutureCallback<HttpResponse> httpCallback) {
    this.healthEndpoint.pollHealth(httpCallback);
  }

  @Override
  public final void close()  {
    if(null != ackEndpoint){
      ackEndpoint.close();
    }
    if(null != eventEndpoint){
    eventEndpoint.close();
    }
    if(null != healthEndpoint){
       healthEndpoint.close();
    }
  }

  @Override
  public synchronized void start() {
    if (started){
      return;
    }
    this.ackEndpoint = createAckEndpoint();
    if(null != ackEndpoint){
      ackEndpoint.start();
    }
    this.eventEndpoint = createEventEndpoint();
    if(null != eventEndpoint){
    eventEndpoint.start();
    }
    this.healthEndpoint = createHealthEndpoint();
    if(null != healthEndpoint){
       healthEndpoint.start();
    }
    started = true;
  }

  protected AckEndpoint createAckEndpoint() {
    return new AckEndpoint();
  }

  protected EventEndpoint createEventEndpoint() {
    return new EventEndpoint(ackEndpoint);
  }

  protected HealthEndpoint createHealthEndpoint() {
    return new HealthEndpoint();
  }
  
}
