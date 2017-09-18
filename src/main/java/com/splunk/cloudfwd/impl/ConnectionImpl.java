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
import com.splunk.cloudfwd.HecConnectionStateException;
import com.splunk.cloudfwd.HecConnectionTimeoutException;
import com.splunk.cloudfwd.HecHealth;

import static com.splunk.cloudfwd.PropertyKeys.*;
import com.splunk.cloudfwd.impl.util.CallbackInterceptor;
import com.splunk.cloudfwd.impl.util.HecChannel;
import com.splunk.cloudfwd.impl.util.LoadBalancer;
import com.splunk.cloudfwd.impl.util.PropertiesFileHelper;
import com.splunk.cloudfwd.impl.util.TimeoutChecker;
import com.splunk.cloudfwd.impl.util.EventBatchLog;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Internal implementation of Connection. Should be obtained through a Factory method. 
 *
 * @author ghendrey
 */
public class ConnectionImpl implements  Connection {

  private static final Logger LOG = LoggerFactory.getLogger(ConnectionImpl.class.getName());

  /**
   * @return the propertiesFileHelper
   */
  public PropertiesFileHelper getPropertiesFileHelper() {
    return propertiesFileHelper;
  }

    @Override
    public ConnectionSettings getSettings() {
        return getPropertiesFileHelper();
    }

  private final LoadBalancer lb;
  private CallbackInterceptor callbacks;
  private TimeoutChecker timeoutChecker;
  private boolean closed;
  private EventBatchImpl events; //default EventBatchImpl used if send(event) is called
  private PropertiesFileHelper propertiesFileHelper;

  public ConnectionImpl(ConnectionCallbacks callbacks) {
    this(callbacks, new Properties());
  }

  public ConnectionImpl(ConnectionCallbacks callbacks, Properties settings) {
    this.propertiesFileHelper = new PropertiesFileHelper(this,settings);
    init(callbacks, propertiesFileHelper);
    this.lb = new LoadBalancer(this);
  }

  private void init(ConnectionCallbacks callbacks, PropertiesFileHelper p) {
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
    this.callbacks = new CallbackInterceptor(callbacks); 

  }

  public long getAckTimeoutMS() {
    return propertiesFileHelper.getAckTimeoutMS();
  }

  public synchronized void setBlockingTimeoutMS(long ms) {
    this.propertiesFileHelper.putProperty(BLOCKING_TIMEOUT_MS, String.
            valueOf(ms));
  }

  @Override
  public void close() {
      flush();
    this.closed = true;
    //we must close asynchronously to prevent deadlocking
    //when close() is invoked from a callback like the
    //Exception handler
    CountDownLatch latch = new CountDownLatch(1);
    new Thread(() -> {
      lb.close();
      timeoutChecker.queisce();
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
    new Thread(() -> {
      lb.closeNow();
      timeoutChecker.queisce();
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
  public synchronized int send(Event event) throws HecConnectionTimeoutException {
    if (null == this.events) {
      this.events = new EventBatchImpl();
    }
    this.events.add(event);
    if (this.events.isFlushable(propertiesFileHelper.getEventBatchSize())) {
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
  public synchronized int sendBatch(EventBatch events) throws HecConnectionTimeoutException {
    if (closed) {
      throw new HecConnectionStateException("Attempt to sendBatch on closed connection.", HecConnectionStateException.Type.SEND_ON_CLOSED_CONNECTION);
    }
    ((EventBatchImpl)events).setSendTimestamp(System.currentTimeMillis());
    //must null the evenbts before lb.sendBatch. If not, event can continue to be added to the 
    //batch while it is in the load balancer. Furthermore, if sending fails, then close() will try to
    //send the failed batch again
    this.events = null; //batch is in flight, null it out.
    //check to make sure the endpoint can absorb all the event formats in the batch
    ((EventBatchImpl)events).checkAndSetCompatibility(propertiesFileHelper.getHecEndpointType());
    timeoutChecker.start();
    timeoutChecker.add((EventBatchImpl)events);
    LOG.debug("sending  characters {} for id {}", events.getLength(),events.getId());
    EventBatchLog.LOG.trace("Sending EventBatch and Starting TimeoutChecker: {}", events);
    lb.sendBatch((EventBatchImpl)events);
    //return the number of characters posted to HEC for the events data
    return events.getLength();
  }

    @Override
  public synchronized void flush() throws HecConnectionTimeoutException {
    if (null != events && events.getNumEvents() != 0) {
      sendBatch(events);
    }
  }

    /**
     * healthCheck will retrieve health for each channel or trigger a health check if no channels have been made
     * @param 
     * @return
     * @throws 
     */
  public synchronized List<HecHealth> healthCheck() {
    if (closed) {
      throw new HecConnectionStateException("Attempt to healthCheck on closed connection.", HecConnectionStateException.Type.SEND_ON_CLOSED_CONNECTION);
    }
    
    return lb.checkHealth();
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
    return propertiesFileHelper.getBlockingTimeoutMS();
  }

  public String getToken() {
    return propertiesFileHelper.getToken();
  }


  public List<URL> getUrls() {
    return propertiesFileHelper.getUrls();
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
    throw new RuntimeException("Not implemeneted");
  }

    /**
     * @return the lb
     */
    public LoadBalancer getLoadBalancer() {
        return lb;
    }
}
