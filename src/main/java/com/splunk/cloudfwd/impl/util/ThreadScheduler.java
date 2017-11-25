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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Maintains a singleton map of ScheduledThreadPoolExecutors, one for each threadpool name.
 *
 * @author ghendrey
 */
public class ThreadScheduler {

  private static final ConcurrentMap<String, ScheduledThreadPoolExecutor> schedulers = new ConcurrentHashMap<>();
  private static final ConcurrentMap<String, ExecutorService> executors = new ConcurrentHashMap<>(); 
  private static final List<ExecutorService> dedicatedSingleThreadExecutors = new ArrayList<>();
  private static final int THREADS_IN_SCHEDULER_POOL = 1;
  private static int MAX_THREADS_IN_EXECUTOR_POOL = Integer.MAX_VALUE; //Pools need to be able to grow large because pre-flight check waits on several latches and will tie up a thread for a long time
  
  
  public synchronized  static ScheduledThreadPoolExecutor getSharedSchedulerInstance(String name){
      return getFromSchedulerCache(name);
  }
  
public synchronized  static ExecutorService getDedicatedSingleThreadExecutor(String name){
       ThreadFactory f = (Runnable r) -> {
                Thread t =  new Thread(r, name);  
                return t;
            };          
        ExecutorService x = Executors.newSingleThreadExecutor(f);
        dedicatedSingleThreadExecutors.add(x);
        return x;
}      
  
  public static ExecutorService getSharedExecutorInstance(String name, int maxThreads){
      return getFromExecutorCache(name, maxThreads);
  }    
  
  public static ExecutorService getSharedExecutorInstance(String name){
      return getFromExecutorCache(name, MAX_THREADS_IN_EXECUTOR_POOL);
  }  
  
    public static synchronized void shutdownNowAndAwaitTermination() {        
        shutdownPools(schedulers.values());
        shutdownPools(executors.values());
        shutdownPools(dedicatedSingleThreadExecutors);
    }   
    
    private static void shutdownPools(Collection<? extends ExecutorService> pool){
        for (ExecutorService execSvc:pool) {
            execSvc.shutdownNow(); 
            try {
                if (!execSvc.isTerminated() && !execSvc.awaitTermination(10,
                        TimeUnit.SECONDS)) {
                    throw new RuntimeException("timed out waiting for scheduler to terminate.");
                }
            } catch (InterruptedException ex) {
                throw new RuntimeException("InterruptedException awating termination of scheduler.");
            }catch(Exception e){
                e.printStackTrace();
            }
        }
        pool.clear();
    }
      

    private static ExecutorService getFromExecutorCache(String name, int maxThreads) {
        return executors.computeIfAbsent(name, k->{
            ThreadFactory f = (Runnable r) -> {
                Thread t =  new Thread(r, name);  
                return t;
            };      
            
            //NOTE - the selection of a fair synchronouse queue is intentional. In particulat, the behavior of a LinkedBlockingQueue
            //is very dangerous, in that, if the core pool threads are somehow stuck (like in awaitNthResponse of ResponseCoordinator)
            //then the pool will simply queue tasks and will NOT expand the number of threads. A synchronous queue, on the other 
            //hand, always results in the allocation of a new thread to the pool when the existing core pool threads are occupied.            
            BlockingQueue q;
            int corePoolSize;
            if(maxThreads < Integer.MAX_VALUE){
                q = new LinkedBlockingQueue(); //causes tasks to be queued when the maxThreads threads in the pool are busy. 
                corePoolSize = maxThreads; //for LinkedBlockingQueue, there will never be more than corePoolSize threads.
            }else{
                q = new SynchronousQueue<>(true); //allows threads in pool to expand to handle tasks. Tasks are never queued
                corePoolSize = 0;
            }

            ThreadPoolExecutor tpe = new ThreadPoolExecutor(corePoolSize, maxThreads,
                                   30L, TimeUnit.SECONDS,
                                   q, f);        
            if(maxThreads < Integer.MAX_VALUE){
                tpe.prestartAllCoreThreads();
            }
            return tpe;
      });
    }
    
    private static ScheduledThreadPoolExecutor getFromSchedulerCache(String name) {
        return schedulers.computeIfAbsent(name, k->{
            ThreadFactory f = (Runnable r) -> {
                Thread t = new Thread(r, name);
                //t.setPriority(Thread.NORM_PRIORITY+1);
                return t;
            };
             ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(THREADS_IN_SCHEDULER_POOL, f);
             scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
             scheduler.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
             scheduler.setRemoveOnCancelPolicy(true); 
             scheduler.setKeepAliveTime(1, TimeUnit.MINUTES); //probably this is not applicable to ScheduledThreadPoolExecutor since it always keeps exactly corePoolSize 
             scheduler.prestartAllCoreThreads();
             return scheduler;
      });
    }    
  
}
