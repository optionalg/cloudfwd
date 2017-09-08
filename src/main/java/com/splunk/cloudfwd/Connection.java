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

import static com.splunk.cloudfwd.PropertyKeys.*;
import com.splunk.cloudfwd.util.CallbackInterceptor;
import com.splunk.cloudfwd.util.LoadBalancer;
import com.splunk.cloudfwd.util.PropertiesFileHelper;
import com.splunk.cloudfwd.util.TimeoutChecker;
import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Represents a reliable Connection to either the "event" HEC endpoint or the
 * "raw" HEc endpoint.
 *
 * @author ghendrey
 */
public class Connection implements Closeable {

  private static final Logger LOG = Logger.getLogger(Connection.class.getName());

  /**
   * @return the propertiesFileHelper
   */
  public PropertiesFileHelper getPropertiesFileHelper() {
    return propertiesFileHelper;
  }


  /**
   * Used to select either structured HEC /event endpoint, or raw HEC endpoint
   */
  public static enum HecEndpoint {
    STRUCTURED_EVENTS_ENDPOINT, RAW_EVENTS_ENDPOINT
  };
  private final LoadBalancer lb;
  private CallbackInterceptor callbacks;
  private TimeoutChecker timeoutChecker;
  private boolean closed;
  private HecEndpoint hecEndpointType;
  private EventBatch events; //default EventBatch used if send(event) is called
  private PropertiesFileHelper propertiesFileHelper;

  public Connection(ConnectionCallbacks callbacks) {
    this(callbacks, new Properties());
  }

  public Connection(ConnectionCallbacks callbacks, Properties settings) {
    this.propertiesFileHelper = new PropertiesFileHelper(settings);
    init(callbacks, propertiesFileHelper);
    this.lb = new LoadBalancer(this);
  }

  private void init(ConnectionCallbacks callbacks, PropertiesFileHelper p) {
    this.events = new EventBatch();
    this.hecEndpointType = HecEndpoint.RAW_EVENTS_ENDPOINT;
    //when callbacks.acknowledged or callbacks.failed is called, in both cases we need to remove
    //the EventBatch that succeeded or failed from the timoutChecker
    this.timeoutChecker = new TimeoutChecker(propertiesFileHelper.getAckTimeoutMS());
    this.callbacks = new CallbackInterceptor(callbacks,
            timeoutChecker::removeEvents);
    this.timeoutChecker.setInterceptor(this.callbacks);
    
  }

  public synchronized void setEventAcknowledgementTimeoutMS(long ms) {
    this.propertiesFileHelper.putProperty(ACK_TIMEOUT_MS, String.valueOf(ms));
  }
  
  public synchronized void setBlockingTimeoutMS(long ms){
    this.propertiesFileHelper.putProperty(BLOCKING_TIMEOUT_MS, String.valueOf(ms));
  }

  @Override
  public void close() {
    try {
      flush();
    } catch (HecConnectionTimeoutException ex) {
      throw new RuntimeException(ex.getMessage(),ex);
    }
    this.closed = true;
    //we must close asynchronously to prevent deadlocking
    //when close() is invoked from a callback like the
    //Exception handler
    CountDownLatch latch = new CountDownLatch(1);
    new Thread(() -> {
      lb.close();
      timeoutChecker.stop();
      latch.countDown();
    }, "Connection Closer").start();
    try {
      latch.await();
    } catch (InterruptedException ex) {
      Logger.getLogger(Connection.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  public void closeNow() {
    this.closed = true;
    //we must close asynchronously to prevent deadlocking
    //when closeNow() is invoked from a callback like the
    //Exception handler
    CountDownLatch latch = new CountDownLatch(1);
    new Thread(() -> {
      lb.closeNow();
      timeoutChecker.stop();
      latch.countDown();
    }, "Connection Closer").start();
    try {
      latch.await();
    } catch (InterruptedException ex) {
      Logger.getLogger(Connection.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  public synchronized int send(Event event) throws HecConnectionTimeoutException {
    if (null == this.events) {
      this.events = new EventBatch();
    }
    this.events.add(event);
    if (this.events.isFlushable(getEventBatchSize())) {
      return sendBatch(events);      
    }
    return 0;

  }

  public synchronized int sendBatch(EventBatch events) throws HecConnectionTimeoutException {
    if (closed) {
      throw new IllegalStateException("Attempt to send on closed channel.");
    }
    timeoutChecker.start();
    timeoutChecker.add(events);
    LOG.info("sending " + events.getLength() + " characters.");
    lb.sendBatch(events);
    this.events = null; //batch is in flight, null it out
    //return the number of characters posted to HEC for the events data
    return events.getLength(); 
  }

  public synchronized void flush() throws HecConnectionTimeoutException {
    if (null != events) {
      sendBatch(events);
    }
  }

  /**
   * @return the callbacks
   */
  public ConnectionCallbacks getCallbacks() {
    return callbacks;
  }

  /**
   * @return the closed
   */
  public boolean isClosed() {
    return closed;
  }

  /**
   * @return the hecEndpointType
   */
  public HecEndpoint getHecEndpointType() {
    return hecEndpointType;
  }

  /**
   * @param hecEndpointType the hecEndpointType to set
   */
  public void setHecEndpointType(
          HecEndpoint hecEndpointType) {
    this.hecEndpointType = hecEndpointType;
  }

  /**
   * @return the desired size of an EventBatch, in characters (not bytes)
   */
  public int getEventBatchSize() {
    return propertiesFileHelper.getEventBatchSize();
  }

  /**
   * @param numChars the size of the EventBatch in characters (not bytes)
   */
  public void setEventBatchSize(int numChars) {
    propertiesFileHelper.putProperty(PropertyKeys.EVENT_BATCH_SIZE, String.valueOf(numChars));
  }
  
  public long getBlockingTimeoutMS(){
    return propertiesFileHelper.getBlockingTimeoutMS();
  }

  public String getToken() {
    return propertiesFileHelper.getToken();
  }

  public void setToken(String token) {
    propertiesFileHelper.putProperty(PropertyKeys.TOKEN, token);
  }

  public void setUrls(String urls) {
    // a single url or a list of comma separated urls
    propertiesFileHelper.putProperty(PropertyKeys.COLLECTOR_URI, urls);
    lb.reloadUrls();
  }

   
}
