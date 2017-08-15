package com.splunk.cloudfwd.util;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class RunOnceScheduler {
    private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private ScheduledFuture<?> countdown = null;
    
    public synchronized void schedule(Runnable task, long delay, TimeUnit units) {
        if (countdown != null) {
          return;
        }
        countdown = scheduler.schedule(task, delay, units);
    }

      public synchronized boolean hasScheduled() {
        return countdown != null;
      }
      
      public synchronized boolean await(long timeout, TimeUnit unit) throws InterruptedException {
    	  return scheduler.awaitTermination(timeout, unit);
      }
      
      public synchronized boolean isDone() {
    	  return (countdown == null) || countdown.isDone();
      }
      
      public synchronized void clear() {
    	  scheduler.shutdownNow();
    	  countdown = null;
      }

}


