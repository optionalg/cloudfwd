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
package com.splunk.cloudfwd.impl.sim.errorgen.ackslost;

import com.splunk.cloudfwd.impl.ConnectionImpl;
import com.splunk.cloudfwd.impl.sim.AckEndpoint;
import com.splunk.cloudfwd.impl.sim.AcknowledgementEndpoint;
import com.splunk.cloudfwd.impl.sim.SimulatedHECEndpoints;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * half of the ack endpoints generated by this endpoint will be lossy. The other half are normal.
 * @author ghendrey
 */
public class LossyEndpoints extends SimulatedHECEndpoints{
  private static final Logger LOG = ConnectionImpl.getLogger(LossyEndpoints.class.getName());
  private static final AtomicInteger count = new AtomicInteger(0); //must be static to insure the counter is global
  
  @Override
  protected AcknowledgementEndpoint createAckEndpoint() {
    if(0 == count.incrementAndGet()%2){ //every other created endpoint will be lossy
      LOG.trace("Add ENDPOINT DEAD");
      return new AckLossyEndpoint();
    }else{
      LOG.trace("Add ENDPOINT LIVE");
      return new AckEndpoint(); //normal
    }
  }
}
