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

import com.splunk.cloudfwd.EventBatch;
import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.ConnectionCallbacks;
import com.splunk.cloudfwd.HecConnectionTimeoutException;
import com.splunk.cloudfwd.HecMaxRetriesException;
import com.splunk.cloudfwd.HecNonStickySessionException;
import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.http.lifecycle.LifecycleEvent;
import com.splunk.cloudfwd.http.ChannelMetrics;
import com.splunk.cloudfwd.http.lifecycle.EventBatchResponse;
import com.splunk.cloudfwd.http.HttpSender;
import com.splunk.cloudfwd.http.lifecycle.LifecycleEventObserver;
import com.splunk.cloudfwd.http.lifecycle.Response;
import com.splunk.cloudfwd.HecIllegalStateException;
import java.io.Closeable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.ExecutorService;

/**
 *
 * @author ghendrey
 */
public class HecChannel implements Closeable, LifecycleEventObserver {

  protected static final Logger LOG = LoggerFactory.getLogger(HecChannel.class.
          getName());

  private ExecutorService ackPollExecutor;
  private final HttpSender sender;
  private final int full;
  private ScheduledExecutorService reaperScheduler; //for scheduling self-removal/shutdown
  private volatile boolean closed;
  private volatile boolean quiesced;
  private volatile boolean healthy = false; // Responsive to indexer 503 "queue full" error.
  private final LoadBalancer loadBalancer;
  private final AtomicInteger unackedCount = new AtomicInteger(0);
  private final AtomicInteger ackedCount = new AtomicInteger(0);
  private final StickySessionEnforcer stickySessionEnforcer = new StickySessionEnforcer();
  private volatile boolean started;
  private final String channelId;
  private final ChannelMetrics channelMetrics;
  private DeadChannelDetector deadChannelDetector;
  private final String memoizedToString;

  public HecChannel(LoadBalancer b, HttpSender sender,
          Connection c) {
    this.loadBalancer = b;
    this.sender = sender;
    this.channelId = newChannelId();
    this.channelMetrics = new ChannelMetrics(c);
    this.channelMetrics.addObserver(this);
    this.full = loadBalancer.getPropertiesFileHelper().
            getMaxUnackedEventBatchPerChannel();
    sender.setChannel(this);
    sender.getHecIOManager().preFlightCheck();
    memoizedToString = this.channelId + "@" + sender.getBaseUrl();
  }

  private static String newChannelId() {
    return java.util.UUID.randomUUID().toString();
  }

  //Occasionally it's an optimization to be able to force ack polling to happen ASAP (vs wait for polling interval).
  //However, we can't directly invoke methods on the HecIOManager as that can lead to a dealock (saw it, not
  //guess about it). What happens is HecIOManager will want to call channelMetrics.ackPollOK, but channelMetrics
  //is also trying to acquire the lock on this object. So deadlock.
  synchronized void pollAcks() {
    ackPollExecutor.execute(sender.getHecIOManager()::pollAcks);
  }

  public synchronized void start() {
    if (started) {
      return;
    }
    //schedule the channel to be automatically quiesced at LIFESPAN, and closed and replaced when empty
    ThreadFactory f = (Runnable r) -> new Thread(r, "Channel Reaper");

    long decomMs = loadBalancer.getPropertiesFileHelper().getChannelDecomMS();
    if (decomMs > 0) {
      reaperScheduler = Executors.newSingleThreadScheduledExecutor(f);
      reaperScheduler.schedule(() -> {
        closeAndReplace();
      }, decomMs, TimeUnit.MILLISECONDS);
    }
    long unresponsiveDecomMS = loadBalancer.getPropertiesFileHelper().
            getUnresponsiveChannelDecomMS();
    if (unresponsiveDecomMS > 0) {
      deadChannelDetector = new DeadChannelDetector(unresponsiveDecomMS);
      deadChannelDetector.start();
    }
    f = (Runnable r) -> new Thread(r, "On-demand Ack Poller");
    this.ackPollExecutor = Executors.newSingleThreadExecutor(f);

    started = true;
  }

  public synchronized boolean send(EventBatch events) {
    if (!isAvailable()) {
      return false;
    }
    if (!started) {
      start();
    }

    //must increment only *after* we exit the blocking condition above
    int count = unackedCount.incrementAndGet();
    LOG.debug("channel=" + getChannelId() + " unack-count=" + count);
    if (!sender.getChannel().equals(this)) {
      String msg = "send channel mismatch: " + this.getChannelId() + " != " + sender.
              getChannel().getChannelId();
      throw new HecIllegalStateException(msg, HecIllegalStateException.Type.CHANNEL_MISMATCH);
    }
    events.setHecChannel(this);
    sender.sendBatch(events);
    if (unackedCount.get() == full) {
      pollAcks();
    }
    return true;
  }

  @Override
  synchronized public void update(LifecycleEvent e) {
    switch (e.getType()) {
      case ACK_POLL_OK: {
        ackReceived(e);
        break;
      }
      case EVENT_POST_OK: {
        //System.out.println("OBSERVED EVENT_POST_OK");
        checkForStickySessionViolation(e);
        break;
      }
      case PREFLIGHT_CHECK_OK:
      case HEALTH_POLL_OK: {
        this.healthy = true; //see isAvailable
        break;
      }
      case ACK_POLL_DISABLED:
        this.healthy = false;
        break;
    }
    if (e instanceof Response) {
      if (((Response) e).getHttpCode() != 200) {
        LOG.warn("Marking channel unhealthy: " + e);
        this.healthy = false;
      }
    }
    if (isAvailable()) {
      loadBalancer.wakeUp();
    }
  }

  private void ackReceived(LifecycleEvent s) {
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
      throw new HecIllegalStateException(msg, HecIllegalStateException.Type.NEGATIVE_UNACKED_COUNT);
    } else if (count == 0) { //we only need to notify when we drop down from FULL. Tighter than syncing this whole method
      if (quiesced) {
        close();
      }
    }
  }

  synchronized void closeAndReplace() {
    if (closed || quiesced) {
      return;
    }
    this.loadBalancer.addChannelFromRandomlyChosenHost(); //add a replacement
    quiesce(); //drain in-flight packets, and close+cancelEventTrackers when empty
  }

  /**
   * Removes channel from load balancer. Remaining data will be sent.
   *
   */
  protected synchronized void quiesce() {
    LOG.debug("Quiescing channel: {}", this);
    quiesced = true;

    if (isEmpty()) {
      close();
    } else {
      pollAcks(); //so we don't have to wait for the next ack polling interval
    }
  }

  synchronized void forceClose() { //wraps internalForceClose in a log messages
    LOG.info("FORCE CLOSING CHANNEL  {}", getChannelId());
    interalForceClose();
  }

  protected void interalForceClose() {
    try {
      this.sender.close();
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
    this.loadBalancer.removeChannel(getChannelId(), true);
    this.channelMetrics.removeObserver(this);
    finishClose();
  }

  @Override
  public synchronized void close() {
    if (closed) {
      LOG.debug("LoggingChannel already closed.");
      return;
    }
    LOG.info("CLOSE channel  {}", this);
    if (!isEmpty()) {
      quiesce(); //this essentially tells the channel to close after it is empty
      return;
    }

    interalForceClose();
  }

  private synchronized void finishClose() {
    this.closed = true;
    if (null != reaperScheduler) {
      reaperScheduler.shutdownNow();
    }
    if (null != deadChannelDetector) {
      deadChannelDetector.close();
    }
    if(null != ackPollExecutor){
      ackPollExecutor.shutdownNow();
    }
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
    return !quiesced && !closed && healthy && this.unackedCount.get() < full;
  }

  @Override
  public String toString() {
    return memoizedToString; //for logging performance we memo-ize the toString
  }

  public Connection getConnection() {
    return this.loadBalancer.getConnection();
  }

  public String getChannelId() {
    return channelId;
  }

  public ConnectionCallbacks getCallbacks() {
    return this.loadBalancer.getConnection().getCallbacks();
  }

  private void checkForStickySessionViolation(LifecycleEvent s) {
    //System.out.println("CHECKING ACKID " + s.getEvents().getAckId());
    this.stickySessionEnforcer.recordAckId(((EventBatchResponse) s).getEvents());
  }

  private class StickySessionEnforcer {

    boolean seenAckIdOne;

    synchronized void recordAckId(EventBatch events) {
      int ackId = events.getAckId().intValue();
      if (ackId == 1) {
        if (seenAckIdOne) {
          Exception e = new HecNonStickySessionException(
                  "ackId " + ackId + " has already been received on channel " + this);
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
            "ChannelDeathChecker", 1);
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
          LOG.warn(
                  "Dead channel detected. Resending messages and force closing channel");
          //synchronize on the load balancer so we do not allow the load balancer to be
          //closed before  resendInFlightEvents. If that
          //could happen, then the channel we replace this one with
          //can be removed before we resendInFlightEvents
          synchronized (loadBalancer) {
            loadBalancer.addChannelFromRandomlyChosenHost(); //add a replacement
            resendInFlightEvents();
            //don't force close until after events resent. 
            //If you do, you will interrupt this very thread when this DeadChannelDetector is shutdownNow()
            //and the events will never send.
            interalForceClose();
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

    @Override
    public void close() {
      deadChannelChecker.stop();
    }

    //take messages out of the jammed-up/dead channel and resend them to other channels
    private void resendInFlightEvents() {
      long timeout = loadBalancer.getConnection().getPropertiesFileHelper().
              getAckTimeoutMS();
      final int maxRetries = loadBalancer.getPropertiesFileHelper().
              getMaxRetries();
      loadBalancer.getConnection().getTimeoutChecker().getUnackedEvents(
              HecChannel.this).forEach((e) -> {
                //Note - in case you are tempted to cancel the checkpoint manager prior to resend, don't. If you do, the 
                //checkpoint can move higher than the event batch you try to resend. That will cause HecIllegalStateException
                //loadBalancer.getCheckpointManager().cancel(e); 
                sender.getHecIOManager().getAcknowledgementTracker().cancel(e);//also need to cancelEventTrackers ack tracker
                if (e.isAcknowledged() || e.isTimedOut(timeout)) {
                  return; //do not resend messages that are in a final state 
                }
                e.prepareToResend(); //we are going to resend it,so mark it not yet flushed
                //we must force messages to be sent because the connection could have been gracefully closed
                //already, in which case sendRoundRobbin will just ignore the sent messages
               boolean forced = true;
                while (true) { //try to resend the message up to N times
                  try {
                    if (e.getNumTries() > maxRetries) {
                      String msg = "Tried to send event id=" + e.
                              getId() + " " + e.getNumTries() + " times.  See property " + PropertyKeys.RETRIES;
                      LOG.warn(msg);
                      loadBalancer.getConnection().getCallbacks().failed(e,
                              new HecMaxRetriesException(msg));
                    } else {
                      LOG.warn("resending  event {}", e);
                      loadBalancer.sendRoundRobin(e, forced);
                    }
                    break;
                  } catch (HecConnectionTimeoutException ex) {
                     LOG.warn("Caught exception resending {}, exception was {}", ex.getMessage());
                  }
                }
              });
      LOG.info("Resent Events from dead channel {}", HecChannel.this);
    }

  }

}
