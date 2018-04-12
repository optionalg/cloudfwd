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
package com.splunk.cloudfwd.impl;

import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.ConnectionCallbacks;
import com.splunk.cloudfwd.ConnectionSettings;
import com.splunk.cloudfwd.Event;
import com.splunk.cloudfwd.EventBatch;
import com.splunk.cloudfwd.HecHealth;
import com.splunk.cloudfwd.error.HecConnectionStateException;
import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import com.splunk.cloudfwd.HecLoggerFactory;
import com.splunk.cloudfwd.error.HecMissingPropertiesException;
import com.splunk.cloudfwd.error.HecNoValidChannelsException;
import com.splunk.cloudfwd.impl.util.CallbackInterceptor;
import com.splunk.cloudfwd.impl.util.CheckpointManager;
import com.splunk.cloudfwd.impl.util.HecChannel;
import com.splunk.cloudfwd.impl.util.LoadBalancer;
import com.splunk.cloudfwd.impl.util.SenderFactory;
import com.splunk.cloudfwd.impl.util.TimeoutChecker;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.splunk.cloudfwd.PropertyKeys.COLLECTOR_URI;
import static com.splunk.cloudfwd.PropertyKeys.TOKEN;

/**
 * Internal implementation of Connection. Should be obtained through a Factory method. 
 *
 * @author ghendrey
 */
public class ConnectionImpl implements Connection {
    
 //memoized loggers   
  private static final Map<String, Logger> loggers = new ConcurrentHashMap<>();    
    
  private HecLoggerFactory loggerFactory;
  private final Logger LOG;
  private final LoadBalancer lb;
  private CheckpointManager checkpointManager; //consolidate metrics across all channels
  private CallbackInterceptor callbacks;
  private TimeoutChecker timeoutChecker;
  private boolean closed;
  private EventBatchImpl events; //default EventBatchImpl used if send(event) is called
  private SenderFactory senderFactory;
  private ConnectionSettings settings;
  private boolean quiesced;


  public ConnectionImpl(ConnectionCallbacks callbacks) {
    this(callbacks, ConnectionSettings.fromPropsFile("/cloudfwd.properties"));
  }

  public ConnectionImpl(ConnectionCallbacks callbacks, ConnectionSettings settings) {
    if (null == callbacks) {
        throw new HecConnectionStateException("ConnectionCallbacks are null",
                HecConnectionStateException.Type.CONNECTION_CALLBACK_NOT_SET);
    }
    if (settings.getToken() == null) {
      throw new HecMissingPropertiesException("Missing required key: " + TOKEN);
    }
    if (settings.getUrlString() == null) {
      throw new HecMissingPropertiesException("Missing required key: " + COLLECTOR_URI);
    }
    // Make sure defaults of -1 are interpreted to their correct values
    settings.setMaxTotalChannels(settings.getMaxTotalChannels());
    
    this.LOG = this.getLogger(ConnectionImpl.class.getName());
    this.settings = settings;
    this.senderFactory = new SenderFactory(this, settings);
    this.checkpointManager = new CheckpointManager(this);
    this.callbacks = new CallbackInterceptor(callbacks, this); //callbacks must be sent before cosntructing LoadBalancer    
    this.lb = new LoadBalancer(this);
    this.events = new EventBatchImpl();
    //when callbacks.acknowledged or callbacks.failed is called, in both cases we need to cancelEventTrackers
    //the EventBatchImpl that succeeded or failed from the timoutChecker
    this.timeoutChecker = new TimeoutChecker(this);
    //when a failure occurs on an EventBatchImpl, everyone who was tracking that event batch needs to cancelEventTrackers
    //tracking that EventBatchImpl. In other words, failed callback should wipe out all trace of the message from
    //the Connection and it becomes the implicit responsibility of the owner of the Connection to resend the
    //Event if they want it delivered. On success, the same thing muse happen - everyone tracking event batch
    //must cancelEventTrackers their tracking. Therefore, we intercept the success and fail callbacks by calling cancelEventTrackers()
    //*before* those two functions (failed, or acknowledged) are invoked.
    if (settings.getConntctionThrowsExceptionOnCreation()) {
      throwExceptionIfNoChannelOK();
    }
    LOG.debug("ConnectionImpl constructor {}", this);
  }
  
  /**
   * @return the SenderFactory
   */
  public SenderFactory getSenderFactory() {
    return senderFactory;
  }

  @Override
  public ConnectionSettings getSettings() {
    return settings;
  }
  
  public CheckpointManager getCheckpointManager() {
    return this.checkpointManager;
  }
  
  public long getAckTimeoutMS() {
    return settings.getAckTimeoutMS();
  }

  public synchronized void setBlockingTimeoutMS(long ms) {
      settings.setBlockingTimeoutMS(ms);
  }
  
   //close() is synchronized, as is send and sendBatch, therefore events cannot be sent before close has returned.
  //After close has returned, any events sent would be rejected because the connection is closed.
  @Override  
  public synchronized void close() {
    if(this.closed){
        return;
    }
    try {
      flush();
    } catch (HecNoValidChannelsException ex) {
      LOG.error("Events could not be flushed on connection close: " +
        ex.getMessage(), ex);
    }
    //wait until after flush to set closed to true (otherwise flush->sendBatch will complain that connection is closed)
    this.closed = true;
    //we must close asynchronously to prevent deadlocking
    //when close() is invoked from a callback like the
    //Exception handler
    CountDownLatch latch = new CountDownLatch(1);
    new Thread(() -> {
      lb.close(); 
      timeoutChecker.queisce();  
     //ThreadScheduler.shutdownNowAndAwaitTermination();      
      latch.countDown();
    }, "Connection Closer").start();
    try {
      latch.await();
    } catch (InterruptedException ex) {
      LOG.error(ex.getMessage(), ex);
    }
  }

    @Override
  public void closeNow() {
    this.closed = true;
    //we must close asynchronously to prevent deadlocking
    //when closeNow() is invoked from a callback like the
    //Exception handler
    CountDownLatch latch = new CountDownLatch(1);
    LOG.debug("ConnectionImpl closeNow {}", this);
    new Thread(() -> {
      lb.closeNow();
      timeoutChecker.closeNow();
      //ThreadScheduler.shutdownNowAndAwaitTermination();
      latch.countDown();
    }, "Connection Closer").start();
    try {
      latch.await();
    } catch (InterruptedException ex) {
      LOG.error(ex.getMessage(), ex);
    }
  }

  /**
   * The send method will send the Event immediately unless buffering is enabled. Buffering is 
 enabled via either the setEventBatchSize method, or the EVENT_BATCH_SIZE property key. The buffer
 is flushed either by closing the Connection, calling flush, or calling send until EVENT_BATCH_SIZE bytes
 have accumulated in the Connections internal EventBatchImpl. When an EventBatchImpl is flushed, the connection's 
 ConnectionCallbacks will be invoked, asynchronusly. The send method may block for up to BLOCKING_TIMEOUT_MS
 milliseconds before throwing  an HecConnecionTimeoutException. 
   * @param event
   * @return the number of bytes sent (will be zero unless buffer reaches EVENT_BATCH_SIZE and flushes)
   * @throws HecConnectionTimeoutException
   * @see com.splunk.cloudfwd.PropertyKeys
   */
  public synchronized int send(Event event) throws HecConnectionTimeoutException, HecNoValidChannelsException {
    if (closed) {
      throw new HecConnectionStateException("Attempt to send on closed connection.", HecConnectionStateException.Type.SEND_ON_CLOSED_CONNECTION);
    }      
    if (null == this.events) {
      this.events = new EventBatchImpl();
    }
    this.events.add(event);
    this.timeoutChecker.start();
    if (this.events.isFlushable(settings.getEventBatchSize())) {
      return sendBatch(events);
    }
    return 0;

  }

  /**
   * sendBatch will immediately send the EventBatchImpl, returning the number of bytes sent, or throws an
 HecConnectionTimeoutException if BLOCKING_TIMEOUT_MS have expired before the batch could be sent. 
   * HecIllegalStateException can be thrown if the connection has already acknowledged an EventBatchImpl with the same id,
 or if an EventBatchImpl with the same id has already previously been sent.
   * @param events
   * @return
   * @throws HecConnectionTimeoutException
   */
    @Override
  public synchronized int sendBatch(EventBatch events) throws HecConnectionTimeoutException, HecNoValidChannelsException {
    if (closed) {
      throw new HecConnectionStateException("Attempt to sendBatch on closed connection.", HecConnectionStateException.Type.SEND_ON_CLOSED_CONNECTION);
    }
   
    // Empty batch, just return
    if (events.getLength() == 0) {
      return 0;
    }
    
    if(LOG.isInfoEnabled()){
        logLBHealth();
    }
    
    ((EventBatchImpl)events).setSendTimestamp(System.currentTimeMillis());
    //must null the evenbts before lb.sendBatch. If not, event can continue to be added to the 
    //batch while it is in the load balancer. Furthermore, if sending fails, then close() will try to
    //send the failed batch again
    this.events = null; //batch is in flight, null it out.
    //check to make sure the endpoint can absorb all the event formats in the batch
    ((EventBatchImpl)events).checkAndSetCompatibility(settings.getHecEndpointType());
    timeoutChecker.start();
    timeoutChecker.add((EventBatchImpl)events);
    LOG.debug("sending  characters {} for id {}", events.getLength(),events.getId());
    lb.sendBatch((EventBatchImpl)events);
    //return the number of characters posted to HEC for the events data
    return events.getLength();
  }

    @Override
  public synchronized void flush() throws HecConnectionTimeoutException, HecNoValidChannelsException {
    if (null != events && events.getNumEvents() != 0) {
      sendBatch(events);
    }
  }

  /**
   * @return the callbacks
   */
    @Override
  public ConnectionCallbacks getCallbacks() {
    return callbacks;
  }

  /**
   * @return the closed
   */
    @Override
  public boolean isClosed() {
    return closed;
  }




  public long getBlockingTimeoutMS() {
    return settings.getBlockingTimeoutMS();
  }

  public String getToken() {
    return settings.getToken();
  }


  public List<URL> getUrls() {
    return settings.getUrls();
  }

  /**
   * @return the TimeoutChecker   
   */
  public TimeoutChecker getTimeoutChecker() {
    return this.timeoutChecker;
  }
  
  public List<EventBatchImpl> getUnackedEvents(HecChannel c){
    return timeoutChecker.getUnackedEvents(c);
  } 
  
  public Collection<EventBatchImpl> getUnackedEvents(){
      return timeoutChecker.getUnackedEvents();
  }
  
    @Override
  public void release(Comparable id){
    throw new RuntimeException("Not implemented");
  }

    /**
     * @return the lb
     */
    public LoadBalancer getLoadBalancer() {
        return lb;
    }

  public void setLoggerFactory(HecLoggerFactory f) {
    loggerFactory = f;
  }

    public Logger getLogger(String name) {
        Logger logger = loggers.get(name); //memoize the loggers
        if (null == logger) {
            if (null != loggerFactory) {
                logger = loggerFactory.getLogger(name);
            } else {
                logger = LoggerFactory.getLogger(name);
            }
            loggers.put(name, logger);
        }
        return logger;
    }

    @Override
    public List<HecHealth> getHealth() {
        return lb.getHealthNonBlocking();
    }
    
    private void throwExceptionIfNoChannelOK()  {      
        List<HecHealth> healths = lb.getHealthNonBlocking(); 
        if(healths.isEmpty()){            
            throw new HecConnectionStateException("No HEC channels could be instatiated on Connection.",
                    HecConnectionStateException.Type.NO_HEC_CHANNELS);
        }   
        if(!lb.anyChannelOK(HecHealth::isHealthy)){   
            //FIXME TODO -- figure out how to close channels without getting ConnectionClosedException when 
            //no data has been sent through the channel yet
            //close all channels since none is healthy
            healths.stream().forEach(health->{health.getChannel().close();});
            
            
            //throw whatever exception caused the first unhealthy channel to be unhealthy
            throw healths.stream().filter(e->!e.isHealthy()).findFirst().get().getStatusException();
        } 
   }  
   
   public EventBatchImpl getUnsentBatch() {
        return this.events;
   }


    private void logLBHealth() {
        List<HecHealth> channelHealths = lb.getHealthNonBlocking();
        int _preflightCompleted=0;
        int _closed=0;
        int _closedFinished=0;
        int _quiesced=0;
        int  _healthy = 0;
        int _full = 0;
        int _misconfigured=0;
        int _dead=0;
        int _decomissioned=0;   
        int _available=0;
        for(HecHealth h:channelHealths){
            LOG.trace("{}", h);
            if(h.getChannel().isPreflightCompleted()){
                _preflightCompleted++;
            }
            if(h.isHealthy()){
                _healthy++;
            }
            if(h.getChannel().isAvailable()){
                _available++;
            }            
            if(h.isFull()){
                _full++;
            }            
            if(h.isMisconfigured()){
                _misconfigured++;
            }
            if(h.getChannel().isQuiesced()){
                _quiesced++;
            }
            if(!h.getTimeSinceDeclaredDead().isZero()){
                _dead++;
            }
            if(!h.getTimeSinceDecomissioned().isZero()){
                _decomissioned++;
            }
            if(h.getChannel().isClosed()){
                _closed++;
            }
            if(h.getChannel().isCloseFinished()){
                _closedFinished++;
            }            
        }
        
        LOG.info("LOAD BALANCER for ConnectionImpl={} : channels={}, preflighted={}, available={}, healthy={}, full={}, quiesced={}, decommed={}, dead={}, closed={}, closedFinished={}, misconfigured={}", 
                this, channelHealths.size(), _preflightCompleted ,_available, _healthy, _full,  _quiesced, _decomissioned, _dead, _closed,_closedFinished, _misconfigured);
    }

}
