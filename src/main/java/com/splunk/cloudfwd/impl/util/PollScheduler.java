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

import com.splunk.cloudfwd.HecIllegalStateException;
import com.splunk.cloudfwd.impl.http.HttpSender;
import com.splunk.cloudfwd.impl.ConnectionImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Periodically delegates polling for acks to the AckManager. Just a simple
 * periodic scheduler.
 *
 * @author ghendrey
 */
public class PollScheduler {

  private Logger LOG = LoggerFactory.getLogger(PollScheduler.class.getName());
  private ScheduledExecutorService scheduler;
  private boolean started;
  private final String name;
  private int corePoolSize = 1;

  public PollScheduler(String name) {
    this.name = name;
  }

  public PollScheduler(String name, int corePoolSize) {
    this(name);
    this.corePoolSize = corePoolSize;
    if (corePoolSize == 0) {
      throw new HecIllegalStateException(
              "Core pool size of zero is dissallowed to do bug https://bugs.openjdk.java.net/browse/JDK-8129861",
              HecIllegalStateException.Type.CORE_POOL_SIZE_ZERO);
    }
  }

  public synchronized void start(Runnable poller, long delay, TimeUnit units) {
    if (started) {
      return;
    }
    ThreadFactory f = (Runnable r) -> new Thread(r, name);
    this.scheduler = Executors.newScheduledThreadPool(corePoolSize, f);
    //NOTE: with fixed *DELAY* NOT scheduleAtFixedRATE. The latter will cause threads to pile up
    //if the execution time of a task exceeds the period. We don't want that.
    scheduler.scheduleWithFixedDelay(poller, 0, delay, units);
    this.started = true;
    LOG.info("STARTED POLLING: " + name + " with interval " + delay + " ms");

  }

  public synchronized boolean isStarted() {
    return started;
  }

  public synchronized void stop() {
    LOG.debug("SHUTTING DOWN POLLER:  " + name);
    if (null != scheduler) {
      scheduler.shutdownNow();
    }
    scheduler = null;
    started = false;
  }

  // Override default logger with per-connection-instance logger
  public void setLogger(ConnectionImpl connection) {
    this.LOG = connection.getLogger(PollScheduler.class.getName());
  }
}
