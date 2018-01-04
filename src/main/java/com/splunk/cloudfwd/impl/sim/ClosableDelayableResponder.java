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

import com.splunk.cloudfwd.impl.util.ThreadScheduler;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
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

    final ExecutorService executor;
    Random rand = new Random(System.currentTimeMillis());

    public ClosableDelayableResponder() {
        ThreadFactory f = new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "simulator_executors");
                //t.setPriority(Thread.NORM_PRIORITY + 1);
                return t;
            }
        };
        //executor = ForkJoinPool.commonPool();
        //executor = ThreadScheduler.getSharedExecutorInstance("simulator_executor");
        //executor = ThreadScheduler.getSharedExecutorInstance("simulator_executor", 1);
        executor = Executors.newSingleThreadExecutor(f);
        //executor = Executors.newScheduledThreadPool(1, f);
        //executor = ThreadScheduler.getDedicatedSingleThreadExecutor("simulator_executor");
            
    }

    protected void delayResponse(Runnable r) {
        try{
            //executor.execute(r);
            Runnable r2 = ()->{
              //ThreadScheduler.getSharedExecutorInstance("simulator_executors").execute(r);
              executor.execute(r);
            };
            ThreadScheduler.getSharedSchedulerInstance("simulator_delayed_response_scheduler").schedule(r2, (long) rand.nextInt(2), TimeUnit.MILLISECONDS);
            //executor.schedule(r2, (long) rand.nextInt(2), TimeUnit.MILLISECONDS);
        }catch(RejectedExecutionException e){
            LOG.trace("rejected delayResponse by {}", Thread.currentThread().getName());
        }
    }
    
  public void close() {      
    LOG.debug("SHUTDOWN EVENT ENDPOINT DELAY SIMULATOR");
    executor.shutdown(); //do not shutdownNOW. If you 'now' it, we interrupt threads/tasks that are still trying to return acks
      try {
          if(!executor.isTerminated() && !executor.awaitTermination(30, TimeUnit.SECONDS)){
              LOG.error("Failed to terminate executor in alloted time.");
          } 
      } catch (InterruptedException ex) {
          LOG.error("Interrupted awaiting termination ClosableDelayedResponder executor.");
      }    

  }

}
