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

import com.splunk.cloudfwd.ConnectionCallbacks;
import com.splunk.cloudfwd.impl.EventBatchImpl;
import com.splunk.cloudfwd.impl.ConnectionImpl;
import com.splunk.cloudfwd.LifecycleEvent;
import com.splunk.cloudfwd.impl.http.ChannelMetrics;
import com.splunk.cloudfwd.impl.http.HttpSender;
import com.splunk.cloudfwd.impl.http.lifecycle.LifecycleEventObserver;
import com.splunk.cloudfwd.impl.http.lifecycle.RequestFailed;
import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.ConnectionSettings;
import com.splunk.cloudfwd.error.HecChannelDeathException;
import com.splunk.cloudfwd.error.HecMaxRetriesException;
import com.splunk.cloudfwd.impl.http.lifecycle.EventBatchHelper;
import java.io.Closeable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import com.splunk.cloudfwd.impl.http.lifecycle.PreflightFailed;
import com.splunk.cloudfwd.error.HecConnectionStateException;
import com.splunk.cloudfwd.error.HecIllegalStateException;
import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import com.splunk.cloudfwd.error.HecNoValidChannelsException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;

import javax.net.ssl.SSLException;
import java.util.List;
import java.util.concurrent.Future;
import java.util.logging.Level;


/**
 *
 * @author ghendrey
 */
public class HecChannel implements Closeable, LifecycleEventObserver {
  private final Logger LOG;
  private final HttpSender sender;
  private final int maxUnackedEvents;
  private ScheduledFuture closeWatchDogTaskFuture;
  private Future onDemandAckPollFuture;
  private Future preflightCheckFuture;
  private volatile boolean closed;
  private volatile boolean quiesced;
  private HecHealthImpl health;
  private final LoadBalancer loadBalancer;
  private final AtomicInteger unackedCount = new AtomicInteger(0);
  private final AtomicInteger ackedCount = new AtomicInteger(0);

  private volatile boolean started;
  private final String channelId;
  private final ChannelMetrics channelMetrics;
  private DeadChannelDetector deadChannelDetector;
  private final String memoizedToString;
  private int preflightCount; //number of times we have sent the preflight checks  
  private boolean preflightCompleted;
  //private volatile boolean closeFinished;
  private CountDownLatch closeFinishedLatched = new CountDownLatch(1);//used to support closeAndFinish which blocks

  public HecChannel(LoadBalancer b, HttpSender sender,
          ConnectionImpl c) throws InterruptedException{
    this.LOG = c.getLogger(HecChannel.class.getName());
    this.loadBalancer = b;
    this.sender = sender;
    this.channelId = newChannelId();
    this.channelMetrics = new ChannelMetrics(c);
    this.channelMetrics.addObserver(this);
    this.maxUnackedEvents = loadBalancer.getConnection().getSettings().getMaxUnackedEventBatchPerChannel();
    this.memoizedToString = this.channelId + "@" + sender.getBaseUrl();
    LOG.info("constructing channel: {}", memoizedToString);
            
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
        if(!health.await(getConnectionSettings().getPreFlightTimeoutMS(), TimeUnit.MILLISECONDS)){
            preFlightTimeout();
        }
        return health;
    }
    
    public boolean isPreflightCompleted(){
        return preflightCompleted;
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
     if(null == onDemandAckPollFuture || onDemandAckPollFuture.isDone()){
           //onDemandAckPoll = ThreadScheduler.getSharedSchedulerInstance("on_demand_ack-poller").schedule(sender.getHecIOManager()::pollAcks, 0, TimeUnit.MILLISECONDS);
            onDemandAckPollFuture = sender.getHecIOManager().pollAcks();           
       }

  }

  public synchronized void start() throws InterruptedException{
    if (started) {
      return;
    }

    preflightCheck();
    setupDeadChannelDetector();

    started = true;
  }


    private void setupDeadChannelDetector() {
        long unresponsiveDecomMS = getConnectionSettings().getUnresponsiveChannelDecomMS();
        if (unresponsiveDecomMS > 0) {
            deadChannelDetector = new DeadChannelDetector(unresponsiveDecomMS);
            deadChannelDetector.start();
        }
    }
  
  private ConnectionSettings getConnectionSettings(){
      return loadBalancer.getConnection().getSettings();
  }

    
 void reapChannel(long decomMs){
      Runnable r = ()->{
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
      };
      //avoid having a many channel decommissionings happening at once (maxThreads=1)
      ThreadScheduler.getSharedExecutorInstance("channel_decom_executor_thread",1).execute(r);
  } 

  public boolean send(EventBatchImpl events) {
    if (!isAvailable()) {
        try {
            Thread.sleep(5000);
        } catch (InterruptedException ex) {
            java.util.logging.Logger.getLogger(HecChannel.class.getName()).
                    log(Level.SEVERE, null, ex);
        }
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
    if (unackedCount.get() == maxUnackedEvents) {
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
          preflightCompleted = true;
          sender.getHecIOManager().startHealthPolling(); //when preflight is OK we can start polling health
    }
    updateHealth(e, wasAvailable);
  }

    private void updateHealth(LifecycleEvent e, boolean wasAvailable) {
        this.health.setStatus(e, e.isOK());
        /*
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
        */
        //when an event batch is NOT successfully delivered we must consider it "gone" from this channel
        if(EventBatchHelper.isEventBatchFailOrNotOK(e)){
            LOG.info("FAIL or NOT OK caused  DECREMENT {}", e);
            this.unackedCount.decrementAndGet();
        }
        
        if (!wasAvailable && isAvailable()) { //channel has become available where as previously NOT available
            LOG.debug("channel became available : channel={}", this);
            loadBalancer.wakeUp(); //inform load balancer so waiting send-round-robin can begin spinning again
        } else if (!isAvailable() && wasAvailable) {
            LOG.debug("channel became unavailable due to {}, channel={}", e.getType(), this);
        }
    }
    
    public boolean isFull(){
        if( this.unackedCount.get()> maxUnackedEvents){
            LOG.error("{} illegal channel state full={}, unackedCount={}", this, maxUnackedEvents, unackedCount.get());
        }
        return this.unackedCount.get() == maxUnackedEvents;
    }

    private void resendPreflight(LifecycleEvent e, boolean wasAvailable) {
        if (e instanceof RequestFailed && e.getException() instanceof SSLException) {
          LOG.warn("PreFlight on channel {} detected exception {}" +
                  ", aborting PreFlight and update health", 
                  this, e.getException());
          updateHealth(new PreflightFailed(e.getException()), wasAvailable);
          this.sender.abortPreflightAndHealthcheckRequests();
          return;
        }
        if (++preflightCount <= getSettings().getMaxPreflightRetries() && !closed && !quiesced) {
            //preflight resends must be decoupled
            //Runnable r = () -> {
                LOG.warn("retrying channel preflight checks on {}",
                        HecChannel.this);
                //try {
                    //we send several requests for preflight checks. This resend can be triggered by failure of any one of them. 
                    //Kill all the others
                    this.sender.abortPreflightAndHealthcheckRequests(); 
//                    this.sender.getHecIOManager().preflightCheck(); //retry preflight check
                    preflightCheck(); //runs in thread from pool
//                } catch (InterruptedException ex) {
//                    LOG.debug("Preflight resend interrupted: {}", ex);
//                }
//            };
           // new Thread(r, "preflight retry " + preflightCount).start();
        } else {
            String msg = this + " could not be started " + PropertyKeys.PREFLIGHT_RETRIES + "="
                    + getSettings().getMaxPreflightRetries() + " exceeded: " + e.getException().getMessage();
            LOG.warn(msg);
            Exception ex = new HecMaxRetriesException(e.getException().getMessage());
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

  public void closeAndReplace() throws InterruptedException{
      closeAndReplace(false);
  }
  
  public void closeAndReplace(boolean force) throws InterruptedException{
    if (!force && (closed || quiesced)) {
      return;
    }
    this.health.decomissioned();
    //must add channel *before* quiesce(). 'cause if channel empty, quiesce proceeds directly to close which will kill terminate
    //the reaperScheduler, which will interrupt this very thread which was spawned by the reaper scheduler, and then  we
    //never get to add the channel.
    HecChannel newChannel = this.loadBalancer.addChannelFromRandomlyChosenHost(force); //add a replacement
    newChannel.getHealth(); //block until this channel is available, before removing old channel
    this.loadBalancer.removeChannel(channelId, true);
    quiesce(); //drain in-flight packets, and close+cancelEventTrackers when empty
    //WE MUST NOT REMOVE THE CHANNEL NOW...MUST GIVE IT CHANCE TO DRAIN AND BE GRACEFULLY REMOVED
    //ONCE IT IS DRAINED. Note that quiesce() call above will start a watchdog thread that will force-remove the channel
    //if it does not gracefully close in 3 minutes.
  }  

  /**
   * Removes channel from load balancer. Remaining data will be sent.
   *
   */
  protected synchronized void quiesce() {
   this.health.unlatch(); //we could be quiescing before pre-flight ever completed, in which case getHealth could block forever
    LOG.debug("Quiescing channel: {}", this);
    long channelQuiesceTimeout = getConnection().getSettings().getChannelQuiesceTimeoutMS();
    
    if(!quiesced){
        this.health.quiesced();
        LOG.debug("Scheduling watchdog to forceClose channel (if needed) in 3 minutes");
        closeWatchDogTaskFuture = ThreadScheduler.getSharedSchedulerInstance("channel_close_watchdog_schedule").
                schedule(this::watchdogClose, channelQuiesceTimeout, TimeUnit.MILLISECONDS);
    }
    quiesced = true;

    if (isEmpty()) {
      close();
    } else {
      pollAcks(); //so we don't have to wait for the next ack polling interval
    }
  }

  private void watchdogClose(){
      Runnable r = ()->{
            if(this.closeFinishedLatched.getCount()!=0){
                LOG.warn("Channel isn't closed. Watchdog will force close it now.");
                HecChannel.this.interalForceClose();
            }else{
                LOG.debug("Channel was closed. Watchdog exiting.");
            }
      };
      ThreadScheduler.getSharedExecutorInstance("watchdog_close_executor").execute(r);
  }

  synchronized public void forceClose() { //wraps internalForceClose in a log messages
    LOG.info("FORCE CLOSING CHANNEL  {}", getChannelId());    
    interalForceClose();
  }

  void interalForceClose() {  
     this.health.unlatch(); //we could be quiescing before pre-flight ever completed, in which case getHealth could block forever
      this.closed = true;
      Runnable r = ()->{
        try {
            LOG.debug("finishing closing channel");
            loadBalancer.removeChannel(getChannelId(), true);
            getSender().getAcknowledgementTracker().kill();
            this.channelMetrics.removeObserver(this);
            cancelTasks(); //make sure all the Excutors are terminated before closing sender (else get ConnectionClosedException)
            this.sender.close();
            closeFinishedLatched.countDown();
            this.sender.abortPreflightAndHealthcheckRequests();             
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
  
  public synchronized void closeAndFinish() {
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
      try {
          closeFinishedLatched.await(1, TimeUnit.MINUTES); //FIXME revisit this...
      } catch (InterruptedException ex) {
          LOG.warn("Interrupted waiting for close to finish.");
      }
  }  

  //do NOT synchronize this method. Since it blocks by awaitTermination it will hold a very long lock on this
  //Object's monitor. This method will get called from a thread kicked off during close. However, the ChannelDeathChecker
  //may also kick in and try to close the channel. But it can't. Because its blocked if this method is synchronized. But this 
  //method would be blocked on awaitTermination waiting for that *very* ChannelDeathChecker thread to terminate. Deadlock.
  private void cancelTasks() {
    LOG.trace("cancelling tasks on  {}", this);
    killAckTracker(); //don't care about any acks that might arrive at this point
    
    sender.getHecIOManager().close(); //shutdown ack and health polling
    sender.abortPreflightAndHealthcheckRequests(); //if any ack and health poll are in flight, abort them
    
    if(null != closeWatchDogTaskFuture && !closeWatchDogTaskFuture.isCancelled()){
        closeWatchDogTaskFuture.cancel(false);
    }
    
    if(null != onDemandAckPollFuture && ! onDemandAckPollFuture.isCancelled()){
        onDemandAckPollFuture.cancel(false);
    }
    
    synchronized(this){ //must synchronize to insure we don't 'lose' a preflight task. it is very important to cancel these preflights.
        if(null != preflightCheckFuture&& ! preflightCheckFuture.isCancelled()){
            preflightCheckFuture.cancel(true); //interrupt preflight
        }
    }

    if (null != deadChannelDetector && !deadChannelDetector.killInProgress) {
      deadChannelDetector.close(); 
    }

  }
  
    /**
     * When called, the channel will ignore any in-flight ack poll responses that might arrive.
     */
    public void killAckTracker(){
      getSender().getHecIOManager().getAcknowledgementTracker().kill();
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
  
  public boolean isClosed(){
      return closed;
  }
  
  public boolean isQuiesced(){
      return quiesced;
  }  
  
  public boolean isCloseFinished(){
      return closeFinishedLatched.getCount()==0;
  }

  /**
   * @return the metrics
   */
  public final ChannelMetrics getChannelMetrics() {
    return this.channelMetrics;
  }

  public boolean isAvailable() {
    return !quiesced && !closed && health.isHealthy() && !isFull();
  }
  
  public boolean isHealthy(){
      return health.isHealthy();
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


    /**
     * @return the sender
     */
    public HttpSender getSender() {
        return sender;
    }

    private synchronized void preflightCheck() {
        preflightCheckFuture = this.sender.getHecIOManager().preflightCheck();
    }
    
 //take messages out of the jammed-up/dead channel and resend them to other channels
    public void resendInFlightEvents() {
        List<EventBatchImpl> unacked = loadBalancer.getConnection().getTimeoutChecker().getUnackedEvents(HecChannel.this);
        LOG.trace("{} events need resending on dead channel {}", unacked.size(), HecChannel.this);       
        AtomicInteger count = new AtomicInteger(0);
        unacked.forEach((e) -> {
                //we must force messages to be sent because the connection could have been gracefully closed
                //already, in which case sendRoundRobbin will just ignore the sent messages
                while (true) { 
                  try {
                      if(!loadBalancer.sendRoundRobin(e, true)){
                          LOG.trace("LoadBalancer did not accept resend of {}", e);
                      }else{
                        LOG.trace("LB ACCEPTED resend of {}", e);
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
    
    public void closeAndReplaceAndResend(){
        try {
            //synchronize on the load balancer so we do not allow the load balancer to be
            //closed before  resendInFlightEvents. If that
            //could happen, then the channel we replace this one with
            //can be removed before we resendInFlightEvents
            closeAndReplace(true); //force=true
        } catch (InterruptedException ex) {
            LOG.error("Exception caught will attempting to closeAndReplace dead channel: {}", ex.getMessage(), ex);
        }
      LOG.warn("Force closing dead channel {}", HecChannel.this);            
      interalForceClose();
      health.dead();            
      resendInFlightEvents();   
    }


  private class DeadChannelDetector implements Closeable {

    private ScheduledFuture task;
    private int lastCountOfAcked;
    private int lastCountOfUnacked;
    private long intervalMS;
    private volatile boolean killInProgress;

    public DeadChannelDetector(long intervalMS) {
      this.intervalMS = intervalMS;
    }

    public synchronized void start() {
      if (null != task) {
        return;
      }

      task = ThreadScheduler.getSharedSchedulerInstance( "channel_death_check_scheduler").
              scheduleWithFixedDelay(this::checkForDeath, 0, intervalMS, TimeUnit.MILLISECONDS);
    }

    private void checkForDeath(){
        if(killInProgress){
            return; //don't allow multiple kills to stack up
        }
        Runnable r = () -> {
        //we here check to see of there has been any activity on the channel since
        //the last time we looked. If not, then we say it was 'frozen' meaning jammed/innactive
        if (unackedCount.get() > 0 && lastCountOfAcked == ackedCount.get()
                && lastCountOfUnacked == unackedCount.get()) {
          killInProgress = true;
          String msg = HecChannel.this  + " dead. Resending "+unackedCount.get()+" unacked messages and force closing channel";
          LOG.warn(msg);
          getCallbacks().systemWarning(new HecChannelDeathException(msg));   
          closeAndReplaceAndResend();
        } else { //channel was not 'frozen'
          lastCountOfAcked = ackedCount.get();
          lastCountOfUnacked = unackedCount.get();
        }
      };//end Runnable
      ThreadScheduler.getSharedExecutorInstance("channel_death_checker_executor").execute(r);
    }
    
    @Override
    public void close() {
        if(null != task && !task.isCancelled()){
            task.cancel(false);
        }
    }

    /**
     * @return the killInProgress
     */
    public boolean isKillInProgress() {
        return killInProgress;
    }
   
  }

}
