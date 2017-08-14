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
import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author ghendrey
 */
public class LoggingChannel implements Closeable, Observer {

  private static final Logger LOG = Logger.getLogger(LoggingChannel.class.
          getName());
  private final HttpEventCollectorSender sender;
  private static final int FULL = 10000; //FIXME TODO set to reasonable value, configurable?
  private ScheduledExecutorService reaperScheduler; //for scheduling self-removal/shutdown
  private static final long LIFESPAN = 60; //5 min lifespan
  private volatile boolean closed;
  private volatile boolean quiesced;
  private final LoadBalancer loadBalancer;
  private final AtomicInteger unackedCount = new AtomicInteger(0);
  private final StickySessionEnforcer stickySessionEnforcer = new StickySessionEnforcer();
  private volatile boolean started;
  private String channelId;

  public LoggingChannel(LoadBalancer b, HttpEventCollectorSender sender) {
    this.loadBalancer = b;
    this.sender = sender;
    this.channelId = newChannelId();

    getChannelMetrics().addObserver(this);

  }

  private static String newChannelId() {
    return java.util.UUID.randomUUID().toString();
  }

  //Occasionally it's an optimization to be able to force ack polling to happen ASAP (vs wait for polling interval).
  //However, we can't directly invoke methods on the AckManager as that can lead to a dealock (saw it, not
  //guess about it). What happens is AckManager will want to call channelMetrics.ackPollOK, but channelMetrics
  //is also trying to acquire the lock on this object. So deadlock.
  synchronized void pollAcks() {
    new Thread(sender.getAckManager()::pollAcks // poll for acks right now
    , "Ack Kicker");
  }

  public synchronized void start() {
    if (started) {
      return;
    }
    sender.setChannel(this);
    //schedule the channel to be automatically quiesced at LIFESPAN, and closed and replaced when empty 
    ThreadFactory f = new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r) {
        return new Thread(r, "Channel Reaper");
      }
    };

    reaperScheduler = Executors.newSingleThreadScheduledExecutor(f);
    reaperScheduler.schedule(() -> {
      closeAndReplace();
    }, LIFESPAN, TimeUnit.SECONDS);
    started = true;
  }

  public synchronized boolean send(EventBatch events) throws TimeoutException {
    if (!isAvailable()) {
      return false;
    }
    if (!started) {
      start();
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
    //System.out.println("Sending to channel: " + sender.getChannel());
    if (unackedCount.get() == FULL) {
      //force an immediate poll for acks, rather than waiting until the next periodically 
      //scheduled ack poll
      pollAcks();
      long start = System.currentTimeMillis();
      while (true) {
        try {
          System.out.println("---BLOCKING---");
          wait(Connection.DEFAULT_SEND_TIMEOUT_MS);
          System.out.println("UNBLOCKED");
        } catch (InterruptedException ex) {
          Logger.getLogger(LoggingChannel.class.getName()).
                  log(Level.SEVERE, null, ex);
        }
        if (System.currentTimeMillis() - start > Connection.DEFAULT_SEND_TIMEOUT_MS) {
          System.out.println("TIMEOUT EXCEEDED");
          throw new TimeoutException("Send timeout exceeded.");
        } else {
          System.out.println("---NO TIMEOUT--");
          break;
        }
      }
    }
    //essentially this is a "double check" since this channel could ge closed while this
    //method was blocked. It happens.It's also why quiesced and closed must be marked volatile
    //so their values are not cached by the thread.
    if (quiesced || closed) {
      return false;
    }
    //must increment only *after* we exit the blocking condition above
    int count = unackedCount.incrementAndGet();
    //System.out.println("channel=" + getChannelId() + " unack-count=" + count);
    sender.sendBatch(events);
    return true;
  }

  @Override
  public synchronized void update(Observable o, Object arg) {
    LifecycleEvent lifecycleEvent = null;
    try {
      lifecycleEvent = (LifecycleEvent) arg;
      LifecycleEvent.Type eventType = ((LifecycleEvent) arg).getType();
      switch (eventType) {
        case ACK_POLL_OK: {
          ackReceived(lifecycleEvent);
          notifyAll();
          break;
        }
        case EVENT_POST_OK: {
          //System.out.println("OBSERVED EVENT_POST_OK");
          checkForStickySessionViolation(lifecycleEvent);
          break;
        }
      }
    } catch (Exception e) {
      LOG.log(Level.SEVERE, e.getMessage(), e);
      FutureCallback c = this.loadBalancer.getConnection().getCallbacks();
      EventBatch events = lifecycleEvent == null ? null : lifecycleEvent.
              getEvents();
      new Thread(() -> {
        c.failed(events, e); //FIXME TODO -- there are many places where we should be calling failed. 
      }).start();

    }
  }

  private void ackReceived(LifecycleEvent s) throws RuntimeException {
    int count = unackedCount.decrementAndGet();
    /*
    System.out.
            println("channel=" + getChannelId() + " unacked-count-post-decr=" + count + " seqno=" + s.
                    getEvents().getId() + " ackid= " + s.getEvents().
                    getAckId());
     */
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
  }

  synchronized void closeAndReplace() {
    if (closed || quiesced) {
      return;
    }
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
    pollAcks();//so we don't have to wait for the next ack polling interval
  }

  synchronized void forceClose() {
    LOG.log(Level.INFO, "FORCE CLOSING CHANNEL  {0}", getChannelId());
    try {
      this.sender.close();
    } catch (Exception e) {
      LOG.log(Level.SEVERE, e.getMessage(), e);
    }
    this.loadBalancer.removeChannel(getChannelId(), true);
    finishClose();
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

    try {
      this.sender.close();
    } catch (Exception e) {
      LOG.log(Level.SEVERE, e.getMessage(), e);
    }
    this.loadBalancer.removeChannel(getChannelId(), false);
    finishClose();
  }

  private synchronized void finishClose() {
    System.out.println("TRYING TO UNBLOCK");
    this.closed = true;
    if (null != reaperScheduler) {
      reaperScheduler.shutdownNow();
    }
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
  public String toString() {
    return this.channelId;
  }

  public Connection getConnection() {
    return this.loadBalancer.getConnection();
  }

  public String getChannelId() {
    return channelId;
  }

  private void checkForStickySessionViolation(LifecycleEvent s) {
    //System.out.println("CHECKING ACKID " + s.getEvents().getAckId());
    this.stickySessionEnforcer.recordAckId(s.getEvents());
  }

  private static class StickySessionEnforcer {

    boolean seenAckIdOne;

    synchronized void recordAckId(EventBatch events) throws IllegalStateException {
      int ackId = events.getAckId().intValue();
      if (ackId == 1) {
        if (seenAckIdOne) {
          throw new IllegalHECAcknowledgementStateException(
                  "ackId " + ackId + " has already been received on channel " + events.
                  getSender().getChannel());
        } else {
          seenAckIdOne = true;
        }
      }
    }
  }

}
