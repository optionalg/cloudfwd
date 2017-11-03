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

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Maintains a singleton map of ScheduledThreadPoolExecutors, one for each threadpool name.
 *
 * @author ghendrey
 */
public class ThreadScheduler {

  //private Logger LOG = LoggerFactory.getLogger(ThreadScheduler.class.getName());
  private static final ConcurrentMap<String, ScheduledThreadPoolExecutor> schedulers = new ConcurrentHashMap<>();
  private static final ConcurrentMap<String, ExecutorService> executors = new ConcurrentHashMap<>(); 
  private static final int MAX_THREADS_IN_SCHEDULER_POOL = 128;
  private static int MAX_THREADS_IN_EXECUTOR_POOL = Integer.MAX_VALUE; //Pools need to be able to grow large because pre-flight check waits on several latches and will tie up a thread for a long time
  
  public synchronized  static ScheduledThreadPoolExecutor getSchedulerInstance(String name){
      return getFromSchedulerCache(name);
  }
  
  public synchronized  static ExecutorService getExecutorInstance(String name){
      return getFromExecutorCache(name);
  }  
  
    public static void shutdownNowAndAwaitTermination() {
        for (ScheduledThreadPoolExecutor scheduler:schedulers.values()) {
            scheduler.shutdownNow();
            try {
                if (!scheduler.isTerminated() && !scheduler.awaitTermination(10,
                        TimeUnit.SECONDS)) {
                    throw new RuntimeException("timed out waiting for scheduler to terminate.");
                }
            } catch (InterruptedException ex) {
                throw new RuntimeException("InterruptedException awating termination of scheduler.");
            }
        }

    }   
    
    public static void shutdownAndAwaitTermination() {
        for (ScheduledThreadPoolExecutor scheduler:schedulers.values()) {
            scheduler.shutdown();
            try {
                if (!scheduler.isTerminated() && !scheduler.awaitTermination(10,
                        TimeUnit.SECONDS)) {
                    throw new RuntimeException("timed out waiting for scheduler to terminate.");
                }
            } catch (InterruptedException ex) {
                throw new RuntimeException("InterruptedException awating termination of scheduler.");
            }
        }

    }       

    private static ExecutorService getFromExecutorCache(String name) {
        return executors.computeIfAbsent(name, k->{
            ThreadFactory f = (Runnable r) -> new Thread(r, name);            
            ThreadPoolExecutor tpe = new ThreadPoolExecutor(2, MAX_THREADS_IN_EXECUTOR_POOL,
                                   30L, TimeUnit.SECONDS,
                                   new LinkedBlockingQueue<Runnable>(), f);
            tpe.prestartAllCoreThreads();
            return tpe;
      });
    }
    
    private static ScheduledThreadPoolExecutor getFromSchedulerCache(String name) {
        return schedulers.computeIfAbsent(name, k->{
            ThreadFactory f = (Runnable r) -> new Thread(r, name);
             ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(MAX_THREADS_IN_SCHEDULER_POOL, f);
             scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
             scheduler.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
             scheduler.setRemoveOnCancelPolicy(true); 
             scheduler.setKeepAliveTime(1, TimeUnit.MINUTES); //probably this is not applicable to ScheduledThreadPoolExecutor since it always keeps exactly corePoolSize 
             scheduler.prestartAllCoreThreads();
             return scheduler;
      });
    }    
  
}
