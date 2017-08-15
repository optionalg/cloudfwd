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
package com.splunk.cloudfwd.util;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Periodically delegates polling. Just a simple periodic scheduler.
 * @author ghendrey
 */
public class PollScheduler {

  private ScheduledExecutorService scheduler;
  private boolean started;
  private final String name;

  public PollScheduler(String name) {
    this.name = name;
  }

  public synchronized void start(Runnable poller, long delay, TimeUnit units) {
    if (started) {
      return;
    }
    ThreadFactory f = new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r) {
        return new Thread(r, name);
      }
    };
    this.scheduler = Executors.newScheduledThreadPool(0, f);
    //NOTE: with fixed *DELAY* NOT scheduleAtFixedRATE. The latter will cause threads to pile up
    //if the execution time of a task exceeds the period. We don't want that.
    scheduler.scheduleWithFixedDelay(poller, 0, delay, units);
    this.started = true;
    System.out.println("STARTED POLLING: " + name);

  }

  public synchronized boolean isStarted() {
    return started;
  }

  public synchronized void stop() {
    System.out.println("SHUTTING DOWN POLLER:  " + name);
    if (null != scheduler) {
      //scheduler.shutdown();
      scheduler.shutdownNow();
    }
    scheduler = null;
    started = false;
  }

}
