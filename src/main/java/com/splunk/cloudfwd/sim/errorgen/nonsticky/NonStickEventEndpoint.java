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

import com.splunk.cloudfwd.sim.AckEndpoint;
import com.splunk.cloudfwd.sim.EventEndpoint;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This AckEndpoint simulates a load balancer that is not sticky. This results
 * in duplicate ackIds being generated for a given channel.
 *
 * @author ghendrey
 */
public class NonStickEventEndpoint extends EventEndpoint {
  private static final Logger LOG = LoggerFactory.getLogger(NonStickEventEndpoint.class.getName());

  public static int NUM_ACK_ENDPOINTS = 2;
  public static int N = 1;
  List<AckEndpoint> ackEndpoints = new ArrayList<>();
  int currentChannelIdx;
  int eventCount;
  AtomicInteger totalCount = new AtomicInteger(0);
  Random rand = new Random(0);
  private boolean started;

  public NonStickEventEndpoint() {
    for (int i = 0; i < NUM_ACK_ENDPOINTS; i++) {
      this.ackEndpoints.add(new AckEndpoint()); //init ackId counters on each channel
    }
  }

  @Override
  public synchronized long nextAckId() {
    LOG.trace("next ack id");
    maybeUnstick();
    return getAckEndpoint().nextAckId();
  }

  private void maybeUnstick() {
    int c = totalCount.incrementAndGet();
    if (0 == c % N) { //every do often...
      //...switch channels
      currentChannelIdx = (++eventCount) % NUM_ACK_ENDPOINTS;
    }
  }

  @Override
  public AckEndpoint getAckEndpoint() {
    AckEndpoint a = ackEndpoints.get(currentChannelIdx);
    LOG.trace("AckEndpoint is " + a);
    return a;
  }

  @Override
  public void close() {
    super.close();
    this.ackEndpoints.forEach(e -> {
      e.close();
    });
  }

  @Override
  public synchronized void start() {
    if(started){
      return;
    }
    super.start();
    this.ackEndpoints.forEach(e -> {
      e.start();
    });
    started = true;
  }

}
