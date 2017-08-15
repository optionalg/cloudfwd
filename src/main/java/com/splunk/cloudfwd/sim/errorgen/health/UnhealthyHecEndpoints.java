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
import com.splunk.cloudfwd.sim.errorgen.slow.SlowEndpoints;
import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;

/**
 * Return a 503 when polling for health
 * 
 * We slow down ack polling so that we can trigger a TimeoutException
 * since ack polling is triggered when HEC is unhealthy
 * 
 * @author meema
 */
public class UnhealthyHecEndpoints extends SlowEndpoints {

	protected long sleep = 5000;
	
  @Override
  public void pollHealth(FutureCallback<HttpResponse> httpCallback) {
      ((AbstractHttpCallback)httpCallback).completed("HEC unhealthy, queues full", 503);
  }
}
