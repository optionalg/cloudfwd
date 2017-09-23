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

import com.splunk.cloudfwd.impl.http.AbstractHttpCallback;
import com.splunk.cloudfwd.impl.sim.AckEndpoint;
import com.splunk.cloudfwd.impl.sim.AcknowledgementEndpoint;
import com.splunk.cloudfwd.impl.sim.CannedEntity;
import com.splunk.cloudfwd.impl.sim.CannedOKHttpResponse;
import com.splunk.cloudfwd.impl.sim.HealthEndpoint;
import com.splunk.cloudfwd.impl.sim.EventEndpoint;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simulate communicating with indexer in manual detention
 * @author meemax
 */
public class SomeInDetentionEndpoints extends InDetentionEndpoints {
  private static final Logger LOG = LoggerFactory.getLogger(SomeInDetentionEndpoints.class.getName());
  private static final AtomicInteger count = new AtomicInteger(0); //must be static to ensure the counter is global
  private boolean inDetention = true;

  @Override
  public synchronized void start() {
    if(0 == count.getAndIncrement() % 2){ //every other created endpoint will be detention
      LOG.trace("put endpoint in detention");
      this.inDetention = true;
    }else{
      LOG.trace("endpoint is live");
      this.inDetention = false;
    }
    super.start();
  }

  @Override
  public void splunkCheck(FutureCallback<HttpResponse> httpCallback) {
    if (this.inDetention) {
      ((AbstractHttpCallback)httpCallback).completed(
          "Not Found",
          404);
    }
    else {
      httpCallback.completed(
          new CannedOKHttpResponse(
            new CannedEntity("Simulated pre-flight check OK")));
    }
  }

  @Override
  protected AcknowledgementEndpoint createAckEndpoint() {
    if (this.inDetention)
      return new InDetentionAckEndpoint();
    else
      return new AckEndpoint();
  }

  @Override
  protected EventEndpoint createEventEndpoint() {
    if (this.inDetention)
      return new InDetentionEventEndpoint();
    else
      return new EventEndpoint(ackEndpoint);
  }

  @Override
  protected HealthEndpoint createHealthEndpoint() {
    if (this.inDetention)
      return new InDetentionHealthEndpoint();
    else
      return new HealthEndpoint();
  }
}
