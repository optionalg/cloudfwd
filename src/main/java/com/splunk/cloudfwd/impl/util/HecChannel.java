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

import com.splunk.cloudfwd.impl.EventBatchImpl;
import com.splunk.cloudfwd.impl.ConnectionImpl;
import com.splunk.cloudfwd.ConnectionCallbacks;
import com.splunk.cloudfwd.HecConnectionTimeoutException;
import com.splunk.cloudfwd.HecNonStickySessionException;
import com.splunk.cloudfwd.LifecycleEvent;
import com.splunk.cloudfwd.impl.http.ChannelMetrics;
import com.splunk.cloudfwd.impl.http.lifecycle.EventBatchResponse;
import com.splunk.cloudfwd.impl.http.HttpSender;
import com.splunk.cloudfwd.impl.http.lifecycle.LifecycleEventObserver;
import com.splunk.cloudfwd.impl.http.lifecycle.Response;
import com.splunk.cloudfwd.HecIllegalStateException;
import com.splunk.cloudfwd.HecHealth;
import com.splunk.cloudfwd.HecMaxRetriesException;
import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.ConnectionSettings;
import java.io.Closeable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import java.util.concurrent.ExecutorService;

/**
 *
 * @author ghendrey
 */
public class HecChannel implements Closeable, LifecycleEventObserver {
  private final Logger LOG;
  private ExecutorService ackPollExecutor;
  private final HttpSender sender;
  private final int full;
  private ScheduledExecutorService reaperScheduler; //for scheduling self-removal/shutdown
  private volatile boolean closed;
  private volatile boolean quiesced;

  //private volatile boolean healthy = false; // Responsive to indexer 503 "queue full" error.
  private HecHealth health;
  
  private final LoadBalancer loadBalancer;
  private final AtomicInteger unackedCount = new AtomicInteger(0);
  private final AtomicInteger ackedCount = new AtomicInteger(0);
  private final StickySessionEnforcer stickySessionEnforcer = new StickySessionEnforcer();
  private volatile boolean started;
  private final String channelId;
  private final ChannelMetrics channelMetrics;
  private DeadChannelDetector deadChannelDetector;
  private final String memoizedToString;
  private int preflightCount; //number of times to retry preflight checks due to 

  public HecChannel(LoadBalancer b, HttpSender sender,
          ConnectionImpl c) {
    this.LOG = c.getLogger(HecChannel.class.getName());
    this.loadBalancer = b;
    this.sender = sender;
    this.channelId = newChannelId();
    this.channelMetrics = new ChannelMetrics(c);
    this.channelMetrics.addObserver(this);
    this.full = loadBalancer.getPropertiesFileHelper().
            getMaxUnackedEventBatchPerChannel();
    this.memoizedToString = this.channelId + "@" + sender.getBaseUrl();
    
    this.health = new HecHealth(sender.getBaseUrl()
        , new LifecycleEvent(LifecycleEvent.Type.PREFLIGHT_HEALTH_CHECK_PENDING));
  }
  
  public HecHealth getHealth() {
    return health;
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
    this.sender.setChannel(this);
    this.sender.getHecIOManager().preflightCheck();

    //schedule the channel to be automatically quiesced at LIFESPAN, and closed and replaced when empty
    ThreadFactory f = (Runnable r) -> new Thread(r, "Channel Reaper");

    long decomMs = loadBalancer.getPropertiesFileHelper().getChannelDecomMS();
    if (decomMs > 0) {
      reaperScheduler = Executors.newSingleThreadScheduledExecutor(f);
      reaperScheduler.schedule(() -> {
          LOG.info("decommissioning channel (channel_decom_ms={}): {}", decomMs, HecChannel.this);
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

  public synchronized boolean send(EventBatchImpl events) {
    if (!started) {
      start();
    }
    if (!isAvailable()) {
      return false;
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
    boolean wasAvailable = isAvailable();
    switch (e.getType()) {
      case ACK_POLL_OK: {
        ackReceived(e);
        break;
      }
      case EVENT_POST_OK: {
        checkForStickySessionViolation(e);
        break;
      }
      case HEALTH_POLL_OK:
      case PREFLIGHT_OK:
      {
        this.health.setStatus(e, true); 
        break;
      }
      case SPLUNK_IN_DETENTION:
      case INDEXER_BUSY:
      case ACK_DISABLED: 
      case INVALID_TOKEN: 
      case INVALID_AUTH: {
        this.health.setStatus(e, false);
        break;
      }
      case EVENT_POST_FAILURE:
      case EVENT_POST_NOT_OK:
      case GATEWAY_TIMEOUT:{
        this.health.setStatus(e, false);
        //when event posts fail we also need to decrement unackedCount because
        //it is a metric of the number of 'in flight' events batches. When an event POST
        //fails, that event batch is no longer in flight on this channel. If we don't do this, channel 
        //can never again become isAvailable()=true,  even if it is healthy because it will 
        //think it is 'full' of unacked events.
        this.unackedCount.decrementAndGet();
        break;         
      }
      case PREFLIGHT_BUSY:
      case PREFLIGHT_GATEWAY_TIMEOUT:
          if(++preflightCount <=getSettings().getMaxPreflightRetries()){
              this.sender.getHecIOManager().preflightCheck(); //retry preflight check
          }else{
              String msg = this + " preflight retried exceeded " + PropertyKeys.PREFLIGHT_RETRIES+"="
                      + getSettings().getMaxPreflightRetries();
              getCallbacks().systemError(new HecMaxRetriesException(msg));
          }
          break;
    }
    //any other non-200 that we have not excplicitly handled above will set the health false
    if (e instanceof Response) {
      if (((Response) e).getHttpCode() != 200) {
        LOG.warn("Marking channel unhealthy: " + e);
        this.health.setStatus(e, false);
      }
    }
    if (!wasAvailable && isAvailable()) { //channel has become available where as previously NOT available
      loadBalancer.wakeUp(); //inform load balancer so waiting send-round-robin can begin spinning again
    }
  }
  
  private ConnectionSettings getSettings(){
      return getConnection().getSettings();
  }

  private void ackReceived(LifecycleEvent s) {
    int count = unackedCount.decrementAndGet();
    ackedCount.incrementAndGet();
    if (count < 0) {
      String msg = "unacked count is illegal negative value: " + count + " on channel " + getChannelId();
      throw new HecIllegalStateException(msg, HecIllegalStateException.Type.NEGATIVE_UNACKED_COUNT);
    } else if (count == 0) { //we only need to notify when we drop down from FULL. Tighter than syncing this whole method
      if (quiesced) {
        close();
      }
    }
  }

  //this cannot be synchronized - it will deadlock when addChannelFromRandomlyChosenHost()
  //tries get the LoadBalancer's monitor but the monitor is held by a thread in LoadBalancer's sendRoundRobin
  //waiting for monitor on this's send.
  void closeAndReplace() {
    if (closed || quiesced) {
      return;
    }
    quiesce(); //drain in-flight packets, and close+cancelEventTrackers when empty
    this.loadBalancer.addChannelFromRandomlyChosenHost(); //add a replacement

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
   * Returns true if this channels has no unacknowledged EventBatchImpl
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
    return !quiesced && !closed && health.isHealthy() && this.unackedCount.get() < full;
  }

  @Override
  public String toString() {
    return memoizedToString; //for logging performance we memo-ize the toString
  }

  public ConnectionImpl getConnection() {
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

    synchronized void recordAckId(EventBatchImpl events) {
      int ackId = events.getAckId().intValue();
      if (ackId == 1) {
        if (seenAckIdOne) {
          Exception e = new HecNonStickySessionException(
                  "ackId " + ackId + " has already been received on channel " + HecChannel.this);
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
      deadChannelChecker.setLogger(sender.getConnection());
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
      loadBalancer.getConnection().getTimeoutChecker().getUnackedEvents(
              HecChannel.this).forEach((e) -> {
                //we must force messages to be sent because the connection could have been gracefully closed
                //already, in which case sendRoundRobbin will just ignore the sent messages
                while (true) { 
                  try {
                      loadBalancer.sendRoundRobin(e, true);
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
