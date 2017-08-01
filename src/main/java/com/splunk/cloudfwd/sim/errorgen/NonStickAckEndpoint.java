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
package com.splunk.cloudfwd.sim.errorgen;

import com.splunk.cloudfwd.sim.AckEndpoint;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This AckEndpoint simulates a load balancer that is not sticky. This results
 * in duplicate ackIds being generated for a given channel.
 *
 * @author ghendrey
 */
public class NonStickAckEndpoint extends AckEndpoint {

  public int modulus = 8;
  List<AtomicLong> ackIdCounters = new ArrayList<>();
  int currentChannelIdx;
  AtomicInteger totalCount = new AtomicInteger(0);

  @Override
  public long nextAckId() {
    int c = totalCount.incrementAndGet();
    if (0 == c % modulus) { //every do often...
      //...switch channels
      currentChannelIdx = (++currentChannelIdx) % ackIdCounters.size();
    }
    //increment the channel we are currently switched to
    long newId = this.ackIdCounters.get(currentChannelIdx).incrementAndGet();
    super.acksStates.put(newId, true); //mock/pretend the events got indexed
    return newId;

  }

}
