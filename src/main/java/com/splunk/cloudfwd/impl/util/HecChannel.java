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
import com.splunk.cloudfwd.LifecycleEvent;
import com.splunk.cloudfwd.impl.http.ChannelMetrics;
import com.splunk.cloudfwd.impl.http.lifecycle.EventBatchResponse;
import com.splunk.cloudfwd.impl.http.HttpSender;
import com.splunk.cloudfwd.impl.http.lifecycle.LifecycleEventObserver;
import com.splunk.cloudfwd.impl.http.lifecycle.Response;
import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.ConnectionSettings;
import com.splunk.cloudfwd.error.HecChannelDeathException;
import com.splunk.cloudfwd.error.HecMaxRetriesException;
import com.splunk.cloudfwd.impl.http.lifecycle.EventBatchHelper;
import com.splunk.cloudfwd.impl.http.lifecycle.Failure;
import java.io.Closeable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import com.splunk.cloudfwd.impl.http.lifecycle.PreflightFailed;
import com.splunk.cloudfwd.error.HecConnectionStateException;
import com.splunk.cloudfwd.error.HecIllegalStateException;
import com.splunk.cloudfwd.error.HecNonStickySessionException;
import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import com.splunk.cloudfwd.error.HecNoValidChannelsException;
import java.util.List;

/**
 *
 * @author ghendrey
 */
public class HecChannel implements Closeable, LifecycleEventObserver {
  private final Logger LOG;
  private final HttpSender sender;
  private final int full;
  private ScheduledExecutorService reaperScheduler; //for scheduling self-removal/shutdown
  private ScheduledExecutorService ackPollExecutor;
  private volatile boolean closed;
  private volatile boolean quiesced;

  private HecHealthImpl health;
  
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
    private boolean closeFinished;

  public HecChannel(LoadBalancer b, HttpSender sender,
          ConnectionImpl c) throws InterruptedException{
    this.LOG = c.getLogger(HecChannel.class.getName());
    this.loadBalancer = b;
    this.sender = sender;
    this.channelId = newChannelId();
    this.channelMetrics = new ChannelMetrics(c);
    this.channelMetrics.addObserver(this);
    this.full = loadBalancer.getPropertiesFileHelper().
            getMaxUnackedEventBatchPerChannel();
    this.memoizedToString = this.channelId + "@" + sender.getBaseUrl();
    
    this.health = new HecHealthImpl(this, new LifecycleEvent(LifecycleEvent.Type.PREFLIGHT_HEALTH_CHECK_PENDING));  
    
    sender.setChannel(this);
//    start();
  }
  
  public void preFlightTimeout() {
      Exception ex = new HecConnectionStateException(this+ " timed out waiting for preflight check to respond.",
              HecConnectionStateException.Type.CHANNEL_PREFLIGHT_TIMEOUT);
      this.health.setStatus(new PreflightFailed(ex), false);
  }

    /**
     * This method will BLOCK until the HecHealth instance receives its first update. As such it should be used with caution
     * and never in a critical section of code that expects to complete quickly.
     * @return
     */
    public HecHealthImpl getHealth() {
        if(!health.await(getConnetionSettings().getPreFlightTimeout(), TimeUnit.MILLISECONDS)){
            preFlightTimeout();
        }
        return health;
    }
    
    public HecHealthImpl getHealthNonblocking() {
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

  public synchronized void start() throws InterruptedException{
    if (started) {
      return;
    }
    // do this setup before the preflight check so they don't get interrupted while executing after a slow preflight
    setupReaper();
    setupDeadChannelDetector();
    setupAckPoller();
    new Thread(()->{
        try {
            this.sender.getHecIOManager().preflightCheck();
        } catch (InterruptedException e){
            LOG.warn("Preflight check interrupted on channel {}", this);
        }
    }, "preflight check on channel " + this).start();
    started = true;
  }

    private void setupAckPoller() {
        ThreadFactory f = (Runnable r) -> new Thread(r, "On-demand Ack Poller");
        this.ackPollExecutor = Executors.newSingleThreadScheduledExecutor(f);
    }

    private void setupDeadChannelDetector() {
        long unresponsiveDecomMS = getConnetionSettings(). getUnresponsiveChannelDecomMS();
        if (unresponsiveDecomMS > 0) {
            deadChannelDetector = new DeadChannelDetector(unresponsiveDecomMS);
            deadChannelDetector.start();
        }
    }
  
  private ConnectionSettings getConnetionSettings(){
      return loadBalancer.getConnection().getSettings();
  }

    private void setupReaper() {
        //schedule the channel to be automatically quiesced at LIFESPAN, and closed and replaced when empty
        ThreadFactory f = (Runnable r) -> new Thread(r, "Channel Reaper");
        long decomMs = getConnetionSettings().getChannelDecomMS();
        if (decomMs > 0) {
            reaperScheduler = Executors.newSingleThreadScheduledExecutor(f);
            long decomTime = (long) (decomMs * (Math.random() + 1));
            reaperScheduler.schedule(() -> {
                LOG.info("decommissioning channel (channel_decom_ms={}): {}",
                        decomMs, HecChannel.this);
                try {
                    closeAndReplace();
                } catch (InterruptedException ex) {
                    LOG.warn("Interrupted trying to close and replace '{}'",
                            HecChannel.this);
                } catch (Exception e) {
                    LOG.error("Exception trying to close and replace '{}': {}",
                            HecChannel.this, e.getMessage());
                }
            }, (long)decomTime, TimeUnit.MILLISECONDS); //randomize the channel decommission - so that all channels do not decomission simultaneously.
        }
    }

  public boolean send(EventBatchImpl events) {
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
   public void update(LifecycleEvent e) {
    if(closed){
        LOG.warn("Discarding {} on CLOSED channel {}", e, this);
        return;
    }
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
      //we don't want to update the health when we get 503/504/fail for preflight; We want to resend preflight
      case PREFLIGHT_BUSY:
      case PREFLIGHT_GATEWAY_TIMEOUT:
      case PREFLIGHT_FAILED:
        resendPreflight(e, wasAvailable);
        return; //don't update health since we did not actually get an 'answer' to our pre-flight check     
      case PREFLIGHT_OK:
          LOG.info("Preflight checks OK on {}", this);
          //Note: we also start polling health if/when we give up on prflight checks due to max retries of preflight failing
          sender.getHecIOManager().startHealthPolling(); //when preflight is OK we can start polling health
    }
    updateHealth(e, wasAvailable);
  }

    private void updateHealth(LifecycleEvent e, boolean wasAvailable) {
        //only health poll  or preflight ok will set health to true
        if(e.getType()==LifecycleEvent.Type.PREFLIGHT_OK || e.getType()==LifecycleEvent.Type.HEALTH_POLL_OK){
            this.health.setStatus(e, true);
        }
        //any other non-200 that we have not excplicitly handled above will set the health false
        if (e instanceof Response) {
            Response r = (Response) e;
            if(!r.isOK()){
                this.health.setStatus(e, false);
            }
        }
        if(e instanceof Failure){
            this.health.setStatus(e, false);
        }
        //when an event batch is NOT successfully delivered we must consider it "gone" from this channel
        if(EventBatchHelper.isEventBatchFailOrNotOK(e)){
            LOG.info("FAIL or NOT OK caused  DECREMENT {}", e);
            this.unackedCount.decrementAndGet();
        }
        
        if (!wasAvailable && isAvailable()) { //channel has become available where as previously NOT available
            loadBalancer.wakeUp(); //inform load balancer so waiting send-round-robin can begin spinning again
        }
    }
    
    public boolean isFull(){
        if( this.unackedCount.get()> full){
            LOG.error("{} illegal channel state full={}, unackedCount={}", this, full, unackedCount.get());
        }
        return  this.unackedCount.get() == full;
    }

    private void resendPreflight(LifecycleEvent e, boolean wasAvailable) {
        if (++preflightCount <= getSettings().getMaxPreflightRetries() && !closed && !quiesced) {
            //preflight resends must be decoupled
            Runnable r = () -> {
                LOG.warn("retrying channel preflight checks on {}",
                        HecChannel.this);
                try {
                    //we send several requests for preflight checks. This resend can be triggered by failure of any one of them. 
                    //Kill all the others
                    this.sender.abortPreflightAndHealthcheckRequests(); 
                    this.sender.getHecIOManager().preflightCheck(); //retry preflight check
                } catch (InterruptedException ex) {
                    LOG.debug("Preflight resend interrupted: {}", ex);
                }
            };
            new Thread(r, "preflight retry " + preflightCount).start();
        } else {
            String msg = this + " could not be started " + PropertyKeys.PREFLIGHT_RETRIES + "="
                    + getSettings().getMaxPreflightRetries() + " exceeded";
            LOG.warn(msg);
            Exception ex = new HecMaxRetriesException(msg);
            updateHealth(new PreflightFailed(ex), wasAvailable);
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
            throw new HecIllegalStateException(msg,
                    HecIllegalStateException.Type.NEGATIVE_UNACKED_COUNT);
        } else if (count == 0) { //we only need to notify when we drop down from FULL. Tighter than syncing this whole method
            if (quiesced) {
                close();
            }
        }
    }

  //this cannot be synchronized - it will deadlock when addChannelFromRandomlyChosenHost()
  //tries get the LoadBalancer's monitor but the monitor is held by a thread in LoadBalancer's sendRoundRobin
  //waiting for monitor on this's send.
  void closeAndReplace() throws InterruptedException{
    if (closed || quiesced) {
      return;
    }
    this.health.decomissioned();
    //must add channel *before* quiesce(). 'cause if channel empty, quiesce proceeds directly to close which will kill terminate
    //the reaperScheduler, which will interrupt this very thread which was spawned by the reaper scheduler, and then  we
    //never get to add the channel.
    this.loadBalancer.addChannelFromRandomlyChosenHost(); //add a replacement
    this.loadBalancer.removeChannel(channelId, true);
    quiesce(); //drain in-flight packets, and close+cancelEventTrackers when empty
    //WE MUST NOT REMOVE THE CHANNEL NOW...MUST GIVE IT CHANCE TO DRAIN AND BE GRACEFULLY REMOVED
    //ONCE IT IS DRAINED. Note that quiesce() call above will start a watchdog thread that will force-remove the channel
    //if it does not gracefully close in 3 minutes.
    //this.loadBalancer.removeChannel(channelId, false); //remove from load balancer

  }

  /**
   * Removes channel from load balancer. Remaining data will be sent.
   *
   */
  protected synchronized void quiesce() {
    LOG.debug("Quiescing channel: {}", this);
    
    if(!quiesced){
        this.health.quiesced();
        LOG.debug("Scheduling watchdog to forceClose channel (if needed) in 3 minutes");
        reaperScheduler.schedule(()->{
            if(!this.closeFinished){
                LOG.warn("Channel isn't closed. Watchdog will force close it now.");
                HecChannel.this.interalForceClose();
            }else{
                LOG.debug("Channel was closed. Watchdog exiting.");
            }
        }, 3, TimeUnit.MINUTES);
    }
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

  void interalForceClose() {  
      this.closed = true;
      Runnable r = ()->{
        try {
            loadBalancer.removeChannel(getChannelId(), true);
            this.channelMetrics.removeObserver(this);
            closeExecutors(); //make sure all the Excutors are terminated before closing sender (else get ConnectionClosedException)
            this.sender.close();
            this.closeFinished = true;
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
      };
      //use thread to shutdown sender. Else we have problem with simulted endpoints where the 
      //http completed callback with the final ackid  (running in a scheduled executor in the ack endpoint)
      //causes HecChannel.close to be called, which leads
      //here. Hence the thread of execution tries to sender.close(), which in tern tries to shutdown the 
     //simulated endpoints. Which means the thread that is shutting down the simulated endpoints IS
     //a thread being executed in the simulated endpoints! hence an interrupted exception in Executor.awaitTermination
     //when the thread awaiting its own demise is terminated.. This decouples it.
     new Thread(r, "Hec Channel " + getChannelId() + " internal resource closer").start();
  }

  @Override
  public synchronized void close() {
    if (closed) {
      LOG.debug("LoggingChannel already closed.");
      return;
    }
    LOG.info("CLOSE channel  {}", this);
    if (!isEmpty()) {
        LOG.trace("{} not empty. Quiescing. unacked count={}", this, unackedCount.get());
      quiesce(); //this essentially tells the channel to close after it is empty
      return;
    }

    interalForceClose();
  }

  //do NOT synchronize this method. Since it blocks by awaitTermination it will hold a very long lock on this
  //Object's monitor. This method will get called from a thread kicked off during close. However, the ChannelDeathChecker
  //may also kick in and try to close the channel. But it can't. Because its blocked if this method is synchronized. But this 
  //method would be blocked on awaitTermination waiting for that *very* ChannelDeathChecker thread to terminate. Deadlock.
  private void closeExecutors() {
    LOG.trace("closing executors on  {}", this);

    if (null != reaperScheduler) {
      reaperScheduler.shutdown(); //do not use the shutdownNOW flavor. Because it causes the reaper-scheduler to get interrupted. 
//      try{
//        if(!reaperScheduler.isTerminated() && !reaperScheduler.awaitTermination(10, TimeUnit.SECONDS)){
//            LOG.error("failed to terminate reaper scheduler.");
//        }
//      }catch(InterruptedException e){
//          LOG.error("AwaitTermination of reaper scheduler interrupted.");
//      }
    }
 
    if (null != deadChannelDetector) {
      deadChannelDetector.close(); 
    }
    if(null != ackPollExecutor && !ackPollExecutor.isTerminated()){
      ackPollExecutor.shutdownNow();
      try{
        if(!ackPollExecutor.isTerminated() && !ackPollExecutor.awaitTermination(10, TimeUnit.SECONDS)){
            LOG.error("failed to terminate on-demand ack poller.");
        }
      }catch(InterruptedException e){
          LOG.error("AwaitTermination of on-demnd ack poller interrupted.");
      }
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
    return !quiesced && !closed && health.isHealthy() && !isFull();
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

    /**
     * @return the sender
     */
    public HttpSender getSender() {
        return sender;
    }

  private class StickySessionEnforcer {

    boolean seenAckIdZero;

    synchronized void recordAckId(EventBatchImpl events) {
      int ackId = events.getAckId().intValue();
      if (ackId == 0) {
        LOG.info("{} Got ackId 0 {}", HecChannel.this, events);
        if (seenAckIdZero) {
          Exception e = new HecNonStickySessionException(
                  "ackId " + ackId + " has already been received on channel " + HecChannel.this);
          HecChannel.this.loadBalancer.getConnection().getCallbacks().failed(
                  events, e);
        } else {
          seenAckIdZero = true;
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
      deadChannelChecker.setLogger(getSender().getConnection());
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
          String msg = HecChannel.this  + " dead. Resending "+unackedCount.get()+" unacked messages and force closing channel";
          LOG.warn(msg);
          quiesce();
          getCallbacks().systemWarning(new HecChannelDeathException(msg));
          //synchronize on the load balancer so we do not allow the load balancer to be
          //closed before  resendInFlightEvents. If that
          //could happen, then the channel we replace this one with
          //can be removed before we resendInFlightEvents
          synchronized (loadBalancer) {
            try{
                loadBalancer.addChannelFromRandomlyChosenHost(); //add a replacement               
            }catch(InterruptedException ex){
                LOG.warn("Unable to replace dead channel: {}", ex);
            }
            resendInFlightEvents(); 
            //don't force close until after events resent. 
            //If you do, you will interrupt this very thread when this DeadChannelDetector is shutdownNow()
            //and the events will never send.
            LOG.warn("Force closing dead channel {}", HecChannel.this);            
            interalForceClose();
            health.dead();
          }
          if(getConnection().isClosed()) {
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
        List<EventBatchImpl> unacked = loadBalancer.getConnection().getTimeoutChecker().getUnackedEvents(HecChannel.this);
        LOG.trace("{} events need resending on dead channel {}", unacked.size(), HecChannel.this);       
        AtomicInteger count = new AtomicInteger(0);
        unacked.forEach((e) -> {
                //we must force messages to be sent because the connection could have been gracefully closed
                //already, in which case sendRoundRobbin will just ignore the sent messages
                while (true) { 
                  try {
                      if(!loadBalancer.sendRoundRobin(e, true)){
                          LOG.trace("LoadBalancer did not accept resend of {} (it was resent max_retries times?)", e);
                      }else{
                        LOG.trace("LB ACCEPTED events");//fixme remove
                        count.incrementAndGet();
                      }
                      break;
                  } catch (HecConnectionTimeoutException|HecNoValidChannelsException ex) {
                     LOG.warn("Caught exception resending {}, exception was {}", ex.getMessage());
                  }
                }
              });
        LOG.info("Resent {} Events from dead channel {}", count,  HecChannel.this);
    }

  }

}
