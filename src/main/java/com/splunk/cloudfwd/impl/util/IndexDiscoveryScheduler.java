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
package com.splunk.cloudfwd.impl.util;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import com.splunk.cloudfwd.impl.ConnectionImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.splunk.cloudfwd.HecIllegalStateException;

/**
 *
 * @author ghendrey
 */
class IndexDiscoveryScheduler {
  private final Logger LOG;
  private IndexDiscoverer discoverer;
  private ScheduledExecutorService scheduler;
  private boolean started;
  private boolean stopped;

  public IndexDiscoveryScheduler(ConnectionImpl c) {
    this.LOG = c.getLogger(IndexDiscoveryScheduler.class.getName());
  }

  public synchronized void start(IndexDiscoverer d){
    if(started){
      throw new HecIllegalStateException("AckPollController already started", HecIllegalStateException.Type.ALREADY_POLLING_ACKS);
    }
    if(stopped){
      LOG.debug("Ignoring request to start stopped IndexDiscoveryScheduler");
      return;
    }
    this.discoverer = d;
        ThreadFactory f = (Runnable r) -> new Thread(r, "IndexDiscovery poller");
    this.scheduler = Executors.newScheduledThreadPool(1, f);
    Runnable poller = () -> {        
          this.discoverer.discover();
    };
    //NOTE: with fixed *DELAY* NOT scheduleAtFixedRATE. The latter will cause threads to pile up
    //if the execution time of a task exceeds the period. We don't want that.
    scheduler.scheduleWithFixedDelay(poller, 0, 1, TimeUnit.SECONDS); //TODO MAKE THIS MILLISECONDS
    this.started = true;
    LOG.info("STARTED INDEX DISCOVERY POLLING");

  }

  synchronized boolean isStarted() {
    return started;
  }

  public synchronized void stop() {
    this.stopped = true;
    if(null == this.scheduler){
      return;
    }
    LOG.info("SHUTTING DOWN INDEX DISCOVER POLLER");
    scheduler.shutdownNow();
    scheduler = null;
  }  
  
}
