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
package com.splunk.cloudfwd.sim.errorgen.health;

import com.splunk.cloudfwd.http.AbstractHttpCallback;
import com.splunk.cloudfwd.sim.HealthEndpoint;
import com.splunk.cloudfwd.util.RunOnceScheduler;

import java.util.concurrent.TimeUnit;

import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;

/**
 *
 * @author meema
 */
public class RecoverHealthEndpoint extends HealthEndpoint{
	  private final long hecRecover = 2000; // 2 second
	  private boolean recovered = false;
	  private RunOnceScheduler scheduler = new RunOnceScheduler();

  public void pollHealth(FutureCallback<HttpResponse> httpCallback) {
	  if (recovered) {
	      ((AbstractHttpCallback)httpCallback).completed("HEC is healthy", 200);
	  }
	  else {
	      ((AbstractHttpCallback)httpCallback).completed("HEC unhealthy, queues full", 503);
	  }
  }

  @Override
  public void close() {
    //no-op for now
  }

  @Override
  public void start() {
	  scheduler.schedule(new Runnable() {
		  @Override
		  public void run() {
			  recovered = true;
		  }
	  }, hecRecover, TimeUnit.MILLISECONDS);
   }
  
}
