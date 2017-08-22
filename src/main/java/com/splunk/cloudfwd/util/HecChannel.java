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

import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.EventBatch;
import com.splunk.cloudfwd.IllegalHECStateException;
import com.splunk.cloudfwd.http.lifecycle.LifecycleEvent;
import com.splunk.cloudfwd.http.ChannelMetrics;
import com.splunk.cloudfwd.http.lifecycle.EventBatchResponse;
import com.splunk.cloudfwd.http.HttpSender;
import com.splunk.cloudfwd.util.PollScheduler;
import com.splunk.cloudfwd.http.lifecycle.LifecycleEventObserver;
import java.io.Closeable;
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
public class HecChannel implements Closeable, LifecycleEventObserver {

  private static final Logger LOG = Logger.getLogger(HecChannel.class.
          getName());
  private final HttpSender sender;
  private static final int FULL = 10000; //FIXME TODO set to reasonable value, configurable?
  private ScheduledExecutorService reaperScheduler; //for scheduling self-removal/shutdown
  private static final long LIFESPAN = 60; //5 min lifespan
  private volatile boolean closed;
  private volatile boolean quiesced;
  private volatile boolean healthy = true; //responsive to indexer 503 "queue full" error
  private volatile boolean receivedFirstEventPostResponse;
  private final LoadBalancer loadBalancer;
  private final AtomicInteger unackedCount = new AtomicInteger(0);
  private final AtomicInteger ackedCount = new AtomicInteger(0);
  private final StickySessionEnforcer stickySessionEnforcer = new StickySessionEnforcer();
  private volatile boolean started;
  private final String channelId;
  private final ChannelMetrics channelMetrics;
  private DeadChannelDetector deadChannelDetector;

  public HecChannel(LoadBalancer b, HttpSender sender,
          Connection c) {
    this.loadBalancer = b;
    this.sender = sender;
    this.channelId = newChannelId();
    this.channelMetrics = new ChannelMetrics(c);
    this.channelMetrics.addObserver(this);
  }

  private static String newChannelId() {
    return java.util.UUID.randomUUID().toString();
  }

  //Occasionally it's an optimization to be able to force ack polling to happen ASAP (vs wait for polling interval).
  //However, we can't directly invoke methods on the HecIOManager as that can lead to a dealock (saw it, not
  //guess about it). What happens is HecIOManager will want to call channelMetrics.ackPollOK, but channelMetrics
  //is also trying to acquire the lock on this object. So deadlock.
  synchronized void pollAcks() {
    new Thread(sender.getHecIOManager()::pollAcks // poll for acks right now
            , "Ack Kicker");
  }

  public synchronized void start() {
    if (started) {
      return;
    }
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
    long decomMS = loadBalancer.getPropertiesFileHelper().
            getUnresponsiveChannelDecomMS();
    if (decomMS > 0) {
      deadChannelDetector = new DeadChannelDetector(decomMS);
      deadChannelDetector.start();
    }

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
    // We want to block and poll for acks if
    // 1) the channel is full
    // OR
    // 2) if we have not successfully posted the first message
    //
    // In case of the first event in the channel, wait() call below will be interruped by
    // notifyAll() call from this.update callback once we get EVENT_POST_OK
    //
    if (unackedCount.get() == FULL
            || (unackedCount.get() != 0 && !receivedFirstEventPostResponse))  {
      //force an immediate poll for acks, rather than waiting until the next periodically
      //scheduled ack poll. DON'T do this if the first batch is in flight still, since
      //we need to wait for 'Set-Cookie' in the response to come back before polling
      //so that we are routed to the correct indexer (if using an external load balancer
      //with sticky sessions)
      if (unackedCount.get() == FULL) {
        pollAcks();
      }
      long start = System.currentTimeMillis();
      while (true) {
        try {
          System.out.println("---BLOCKING---");
          wait(Connection.DEFAULT_SEND_TIMEOUT_MS);
          System.out.println("UNBLOCKED");
        } catch (InterruptedException ex) {
          Logger.getLogger(HecChannel.class.getName()).
                  log(Level.SEVERE, ex.getMessage(), ex);
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
    if (quiesced || closed || !healthy)  {
      return false;
    }
    //must increment only *after* we exit the blocking condition above
    int count = unackedCount.incrementAndGet();
    //System.out.println("channel=" + getChannelId() + " unack-count=" + count);
    if (!sender.getChannel().equals(this)) {
      String msg = "send channel mismatch: " + this.getChannelId() + " != " + sender.
              getChannel().getChannelId();
      LOG.severe(msg);
      throw new IllegalStateException(msg);
    }
    sender.sendBatch(events);
    return true;
  }

  @Override
  synchronized public void update(LifecycleEvent e) {
    switch (e.getType()) {
      case ACK_POLL_OK: {
        ackReceived(e);
        notifyAll();
        break;
      }
      case EVENT_POST_OK: {
        //System.out.println("OBSERVED EVENT_POST_OK");

        // the below if clause is to unblock sending in the channel once we get post OK
        // for the first event in the channel
        if (!receivedFirstEventPostResponse) {
          System.out.println("channel=" + getChannelId() +
                  ", received OBSERVED EVENT_POST_OK and unblocking sending" +
                  " for the first event in the channel");
          notifyAll();
          receivedFirstEventPostResponse = true;
        }
        checkForStickySessionViolation(e);
        break;
      }
      case HEALTH_POLL_NOT_OK:{
        this.healthy = false;//see isAvailable
        notifyAll();
        break;
      }
      case HEALTH_POLL_OK:{
        this.healthy = true; //see isAvailable
        notifyAll();
        break;
      }
    }
  }

  private void ackReceived(LifecycleEvent s) throws RuntimeException {
    int count = unackedCount.decrementAndGet();
    ackedCount.incrementAndGet();
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

  synchronized void forceCloseAndReplace() {
    this.loadBalancer.addChannelFromRandomlyChosenHost(); //add a replacement
    interalForceClose();
  }

  synchronized void forceClose() {
    LOG.log(Level.INFO, "FORCE CLOSING CHANNEL  {0}", getChannelId());
    interalForceClose();
  }

  private void interalForceClose() {
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
      LOG.info("LoggingChannel already closed.");
      return;
    }
    LOG.log(Level.INFO, "CLOSE {0}", getChannelId());
    if (!isEmpty()) {
      quiesce(); //this essentially tells the channel to close after it is empty
      return;
    }

    forceClose();
  }

  private synchronized void finishClose() {
    System.out.println("TRYING TO UNBLOCK");
    this.closed = true;
    if (null != reaperScheduler) {
      reaperScheduler.shutdownNow();
    }
    if (null != deadChannelDetector) {
      deadChannelDetector.close();
    }
    notifyAll();

  }

  /**
   * Returns true if this channels has no unacknowledged EventBatch
   *
   * @return true if ackwindow is empty
   */
  public boolean isEmpty() {
    return this.unackedCount.get() == 0;
  }

  int getUnackedCount() {
    return this.unackedCount.get();
  }

  /**
   * @return the metrics
   */
  public final ChannelMetrics getChannelMetrics() {
    return this.channelMetrics;
  }

  boolean isAvailable() {
    ChannelMetrics metrics = sender.getChannelMetrics();
    return !quiesced && !closed && healthy && this.unackedCount.get() < FULL; //FIXME TODO make configurable   
  }

  @Override
  public String toString() {
    return this.channelId + "@" + sender.getBaseUrl();
  }

  public Connection getConnection() {
    return this.loadBalancer.getConnection();
  }

  public String getChannelId() {
    return channelId;
  }

  private void checkForStickySessionViolation(LifecycleEvent s) {
    //System.out.println("CHECKING ACKID " + s.getEvents().getAckId());
    this.stickySessionEnforcer.recordAckId(((EventBatchResponse) s).getEvents());
  }

  private class StickySessionEnforcer {

    boolean seenAckIdOne;

    synchronized void recordAckId(EventBatch events) throws IllegalStateException {
      int ackId = events.getAckId().intValue();
      if (ackId == 1) {
        if (seenAckIdOne) {
          Exception e = new IllegalHECStateException(
                  "ackId " + ackId + " has already been received on channel " + events.
                  getSender().getChannel());
          HecChannel.this.loadBalancer.getConnection().getCallbacks().failed(
                  events, e);
        } else {
          seenAckIdOne = true;
        }
      }
    }
  }

  private class DeadChannelDetector implements Closeable {

    private PollScheduler deadChannelChecker = new PollScheduler(
            "ChannelDeathChecker", 0);
    private int lastCountOfAcked;
    private int lastCountOfUnacked;
    private boolean started;
    private long intervalMS;

    public DeadChannelDetector(long intervalMS) {
      this.intervalMS = intervalMS;
    }

    public synchronized void start() {
      if (started) {
        return;
      }
      started = true;
      Runnable r = () -> {
        //we here check to see of there has been any activity on the channel since
        //the last time we looked. If not, then we say it was 'frozen' meaning jammed/innactive
        if (unackedCount.get() > 0 && lastCountOfAcked == ackedCount.get()
                && lastCountOfUnacked == unackedCount.get()) {
          LOG.severe(
                  "Dead channel detected. Resending messages and force closing channel");
          //synchronize on the load balancer so we do not allow the load balancer to be 
          //closed between forceCloseAndReplace and resendInFlightEvents. If that
          //could happen, then the channel we replace this one with in forceCloseAndReplace
          //can be removed before we resendInFlightEvents
          synchronized(loadBalancer){
            forceCloseAndReplace();  //we kill this dead channel but must replace it with a new channel
            resendInFlightEvents();
          }
          if (sender.getConnection().isClosed()) {
            loadBalancer.close();
          }
        } else { //channel was not 'frozen'
          lastCountOfAcked = ackedCount.get();
          lastCountOfUnacked = unackedCount.get();
        }
      };
      deadChannelChecker.start(r, intervalMS, TimeUnit.MILLISECONDS);
    }

    public void close() {
      deadChannelChecker.stop();
    }

    //take messages out of the jammed-up/dead channel and resend them to other channels
    private void resendInFlightEvents() {
      sender.getAcknowledgementTracker().getAllInFlightEvents().forEach((e) -> {
          e.prepareToResend(); //we are going to resend it,so mark it not yet flushed
          //we must force messages to be sent because the connection could have been gracefully closed
          //already, in which case sendRoundRobbin will just ignore the sent messages
          boolean forced = true;
          loadBalancer.sendRoundRobin(e, forced);
      });

    }

  }

}
