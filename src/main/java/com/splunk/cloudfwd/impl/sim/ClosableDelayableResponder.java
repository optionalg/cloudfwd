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
package com.splunk.cloudfwd.impl.sim;

import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author ghendrey
 */
public class ClosableDelayableResponder {
     private static final Logger LOG = LoggerFactory.getLogger(EventEndpoint.class.getName());

    final ScheduledExecutorService executor;
    Random rand = new Random(System.currentTimeMillis());

    public ClosableDelayableResponder() {
        ThreadFactory f = new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "EventEndpoint");
            }
        };
        executor = Executors.newScheduledThreadPool(1, f);
    }

    protected void delayResponse(Runnable r) {
        //return a single response with a delay uniformly distributed between  [0,5] ms
        try{
            executor.schedule(r, (long) rand.nextInt(2), TimeUnit.MILLISECONDS);
        }catch(RejectedExecutionException e){
            LOG.trace("rejected delayResponse by {}", Thread.currentThread().getName());
        }
    }
    
  public void close() {
    LOG.debug("SHUTDOWN EVENT ENDPOINT DELAY SIMULATOR");
    executor.shutdownNow();
      try {
          if(!executor.isTerminated() && !executor.awaitTermination(10, TimeUnit.SECONDS)){
              LOG.error("Failed to terminate executor in alloted time.");
          } } catch (InterruptedException ex) {
          LOG.error("Interrupted awaiting termination ClosableDelayedResponder executor.");
      }    
  }

}
