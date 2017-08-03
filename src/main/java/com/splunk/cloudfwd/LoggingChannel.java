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
package com.splunk.cloudfwd;

import com.splunk.cloudfwd.http.LifecycleEvent;
import com.splunk.cloudfwd.http.ChannelMetrics;
import com.splunk.cloudfwd.http.EventBatch;
import com.splunk.cloudfwd.http.HttpEventCollectorSender;
import java.io.Closeable;
import java.util.Objects;
import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author ghendrey
 */
public class LoggingChannel implements Closeable, Observer {

  public static final int ACK_DUP_DETECTOR_WINDOW_SIZE = 10000;
  private static final Logger LOG = Logger.getLogger(LoggingChannel.class.
          getName());
  private final HttpEventCollectorSender sender;
  private static final int FULL = 500; //FIXME TODO set to reasonable value, configurable?
  private static final ScheduledExecutorService reaperScheduler = Executors.
          newScheduledThreadPool(1); //for scheduling self-removal/shutdown
  private static final long LIFESPAN = 5 * 60 * 1000; //5 min lifespan
  private volatile boolean closed;
  private volatile boolean quiesced;
  private final LoadBalancer loadBalancer;
  private final AtomicInteger unackedCount = new AtomicInteger(0);
  private final StickySessionEnforcer stickySessionEnforcer = new StickySessionEnforcer();

  public LoggingChannel(LoadBalancer b, HttpEventCollectorSender sender) {
    this.loadBalancer = b;
    this.sender = sender;
    getChannelMetrics().addObserver(this);
    //schedule the channel to be automatically quiesced at LIFESPAN, and closed and replaced when empty 
    reaperScheduler.schedule(() -> {
      closeAndReplace();
    }, LIFESPAN, TimeUnit.MILLISECONDS);

  }

  public synchronized boolean send(EventBatch events) throws TimeoutException {
    if (!isAvailable()) {
      return false;
    }
    if (this.closed) {
      LOG.severe("Attempt to send to closed channel");
      throw new IllegalStateException(
              "Attempt to send to quiesced/closed channel");
    }
    if (this.quiesced) {
      LOG.
              info("Send to quiesced channel (this should happen from time to time)");
    }
    System.out.println("Sending to channel: " + sender.getChannel());
    if (unackedCount.get() == FULL) {
      long start = System.currentTimeMillis();
      while (true) {
        try {
          System.out.println("---BLOCKING---");
          wait(Connection.SEND_TIMEOUT);
        } catch (InterruptedException ex) {
          Logger.getLogger(LoggingChannel.class.getName()).
                  log(Level.SEVERE, null, ex);
        }
        if (System.currentTimeMillis() - start > Connection.SEND_TIMEOUT) {
          System.out.println("TIMEOUT EXCEEDED");
          throw new TimeoutException("Send timeout exceeded.");
        } else {
          System.out.println("---UNBLOCKED--");
          break;
        }
      }
    }
    //essentially this is a "double check" since this channel could be closed while this
    //method was blocked. It happens.
    if (quiesced || closed) {
      return false;
    }
    //must increment only *after* we exit the blocking condition above
    int count = unackedCount.incrementAndGet();
    System.out.println("channel=" + getChannelId() + " unack-count=" + count);
    sender.sendBatch(events);
    return true;
  }

  @Override
  public synchronized void update(Observable o, Object arg) {
    try {
      LifecycleEvent s = (LifecycleEvent) arg;
      LifecycleEvent.Type eventType = ((LifecycleEvent) arg).getCurrentState();
      switch (eventType) {
        case ACK_POLL_OK: {
          ackReceived(s);
          break;
        }
        case EVENT_POST_OK: {
          System.out.println("OBSERVED EVENT_POST_OK");
          checkForStickySessionViolation(s);
          break;
        }
      }
    } catch (Exception e) {
      LOG.log(Level.SEVERE, e.getMessage(), e);
      Consumer c = this.loadBalancer.getConnection().getExceptionHandler();
      if(c != null){
        c.accept(e); 
      }
      forceClose();
    }
  }

  private void ackReceived(LifecycleEvent s) throws RuntimeException {
    int count = unackedCount.decrementAndGet();
    System.out.
            println("channel=" + getChannelId() + " unacked-count-post-decr=" + count + " seqno=" + s.
                    getEvents().getId() + " ackid= " + s.getEvents().
                    getAckId());
    if (count < 0) {
      String msg = "unacked count is illegal negative value: " + count + " on channel " + getChannelId();
      LOG.severe(msg);
      throw new RuntimeException(msg);
    } else if (count == 0) { //we only need to notify when we drop down from FULL. Tighter than syncing this whole method
      if (quiesced) {
        try {
          close();
        } catch (IllegalStateException ex) {
          LOG.warning(
                  "unable to close channel " + getChannelId() + ": " + ex.
                  getMessage());
        }
      }
    }
    System.out.println("UNBLOCK");
    notifyAll();
  }

  synchronized void closeAndReplace() {
    this.loadBalancer.addChannelFromRandomlyChosenHost(); //add a replacement
    quiesce(); //drain in-flight packets, and close+remove when empty 
  }

  /**
   * Removes channel from load balancer. Remaining data will be sent.
   *
   */
  protected synchronized void quiesce() {
    LOG.log(Level.INFO, "Quiescing channel: {0}", getChannelId());
    quiesced = true;
  }

  private synchronized void forceClose(){
    LOG.log(Level.INFO, "FORCE CLOSING CHANNEL  {0}", getChannelId());
    this.sender.close();
    this.loadBalancer.removeChannel(getChannelId());
    System.out.println("TRYING TO UNBLOCK");
    this.closed = true;
    notifyAll();    
  }
  @Override
  public synchronized void close() {
    if (closed) {
      LOG.severe("LoggingChannel already closed.");
      return;
      //throw new IllegalStateException("LoggingChannel already closed.");
    }
    LOG.log(Level.INFO, "CLOSE {0}", getChannelId());
    if (!isEmpty()) {
      quiesce(); //this essentially tells the channel to close after it is empty
      return;
    }

    this.sender.close();
    this.loadBalancer.removeChannel(getChannelId());
    System.out.println("TRYING TO UNBLOCK");
    //getChannelMetrics().deleteObserver(this);
    //getChannelMetrics().deleteObserver(this.loadBalancer.getConnectionState());
    this.closed = true;
    notifyAll();
  }

  /**
   * Returns true if this channels has no unacknowledged EventBatch
   *
   * @return true if ackwindow is empty
   */
  protected boolean isEmpty() {
    return this.unackedCount.get() == 0;
  }

  int getUnackedCount() {
    return this.unackedCount.get();
  }

  /**
   * @return the metrics
   */
  public final ChannelMetrics getChannelMetrics() {
    return sender.getChannelMetrics();
  }

  boolean isAvailable() {
    ChannelMetrics metrics = sender.getChannelMetrics();
    return !quiesced && !closed && metrics.getUnacknowledgedCount() < FULL; //FIXME TODO make configurable   
  }

  @Override
  public int hashCode() {
    int hash = 3;
    hash = 19 * hash + Objects.hashCode(this.sender.getChannel());
    return hash;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final LoggingChannel other = (LoggingChannel) obj;
    return Objects.equals(this.sender.getChannel(), other.sender.getChannel());
  }

  String getChannelId() {
    return sender.getChannel();
  }

  private void checkForStickySessionViolation(LifecycleEvent s) {
    System.out.println("CHECKING ACKID " + s.getEvents().getAckId());
    this.stickySessionEnforcer.recordAckId(s.getEvents());
  }

  private static class StickySessionEnforcer {
    boolean seenAckIdOne;

    synchronized void recordAckId(EventBatch events) throws IllegalStateException {
      int ackId = events.getAckId().intValue();
      if(ackId==1){
        if(seenAckIdOne){
        throw new IllegalHECAcknowledgementStateException(
                "ackId " + ackId + " has already been received on channel " + events.
                getSender().getChannel());        
        }else{
          seenAckIdOne = true;
        }
      }
    }
  }

}
