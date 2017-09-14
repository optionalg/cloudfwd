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
import com.splunk.cloudfwd.util.HecChannel;
import com.splunk.cloudfwd.util.LoadBalancer;
import com.splunk.cloudfwd.util.PropertiesFileHelper;
import com.splunk.cloudfwd.util.TimeoutChecker;
import java.io.Closeable;
import java.net.URL;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a reliable Connection to either the /event HEC endpoint or the
 * /raw HEC endpoint.
 *
 * @author ghendrey
 */
public class Connection implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(Connection.class.
          getName());

  /**
   * @return the propertiesFileHelper
   */
  public PropertiesFileHelper getPropertiesFileHelper() {
    return propertiesFileHelper;
  }

  /**
   * Used to select either structured HEC /event endpoint or /raw HEC endpoint
   */
  public static enum HecEndpoint {
    STRUCTURED_EVENTS_ENDPOINT, RAW_EVENTS_ENDPOINT
  };
  private final LoadBalancer lb;
  private CallbackInterceptor callbacks;
  private TimeoutChecker timeoutChecker;
  private boolean closed;
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
    //when callbacks.acknowledged or callbacks.failed is called, in both cases we need to cancelEventTrackers
    //the EventBatch that succeeded or failed from the timoutChecker
    this.timeoutChecker = new TimeoutChecker(this);
    //when a failure occurs on an EventBatch, everyone who was tracking that event batch needs to cancelEventTrackers
    //tracking that EventBatch. In other words, failed callback should wipe out all trace of the message from 
    //the Connection and it becomes the implicit responsibility of the owner of the Connection to resend the
    //Event if they want it delivered. On success, the same thing muse happen - everyone tracking event batch
    //must cancelEventTrackers their tracking. Therefore, we intercept the success and fail callbacks by calling cancelEventTrackers()
    //*before* those two functions (failed, or acknowledged) are invoked.
    this.callbacks = new CallbackInterceptor(callbacks); 

  }

  /**
   * Set event acknowledgement timeout. See PropertyKeys.ACK_TIMEOUT_MS
   * for more information.
   * @param ms
   */
  public synchronized void setAckTimeoutMS(long ms) {
    if (ms != propertiesFileHelper.getAckTimeoutMS()) {
      this.propertiesFileHelper.putProperty(ACK_TIMEOUT_MS, String.valueOf(ms));
      this.timeoutChecker.setTimeout(ms);
    }
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
   * enabled via either the setEventBatchSize method, or the EVENT_BATCH_SIZE property key. The buffer
   * is flushed either by closing the Connection, calling flush, or calling send until EVENT_BATCH_SIZE bytes
   * have accumulated in the Connections internal EventBatch. When an EventBatch is flushed, the connection's 
   * ConnectionCallbacks will be invoked, asynchronusly. The send method may block for up to BLOCKING_TIMEOUT_MS
   * milliseconds before throwing  an HecConnecionTimeoutException. 
   * @param event
   * @return the number of bytes sent (will be zero unless buffer reaches EVENT_BATCH_SIZE and flushes)
   * @throws HecConnectionTimeoutException
   * @see com.splunk.cloudfwd.PropertyKeys
   */
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

  /**
   * sendBatch will immediately send the EventBatch, returning the number of bytes sent, or throws an
   * HecConnectionTimeoutException if BLOCKING_TIMEOUT_MS have expired before the batch could be sent. 
   * HecIllegalStateException can be thrown if the connection has already acknowledged an EventBatch with the same id,
   * or if an EventBatch with the same id has already previously been sent.
   * @param events
   * @return
   * @throws HecConnectionTimeoutException
   */
  public synchronized int sendBatch(EventBatch events) throws HecConnectionTimeoutException {
    if (closed) {
      throw new HecConnectionStateException("Attempt to sendBatch on closed connection.", HecConnectionStateException.Type.SEND_ON_CLOSED_CONNECTION);
    }
    //must null the events before lb.sendBatch. If not, event can continue to be added to the
    //batch while it is in the load balancer. Furthermore, if sending fails, then close() will try to
    //send the failed batch again
    this.events = null; //batch is in flight, null it out.
    //check to make sure the endpoint can absorb all the event formats in the batch
    events.checkAndSetCompatibility(getHecEndpointType());
    timeoutChecker.start();
    timeoutChecker.add(events);
    LOG.debug("sending  characters {} for id {}", events.getLength(),events.getId());
    lb.sendBatch(events);
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
    return propertiesFileHelper.getHecEndpointType();
  }

  /**
   * @param hecEndpointType the hecEndpointType to set
   */
  public void setHecEndpointType(
          HecEndpoint hecEndpointType) {
    propertiesFileHelper.setHecEndpointType(hecEndpointType);
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
    propertiesFileHelper.putProperty(PropertyKeys.EVENT_BATCH_SIZE, String.
            valueOf(numChars));
  }

  public long getBlockingTimeoutMS() {
    return propertiesFileHelper.getBlockingTimeoutMS();
  }

  public String getToken() {
    return propertiesFileHelper.getToken();
  }

  /**
   * Set Http Event Collector token to use.
   * May take up to PropertyKeys.CHANNEL_DECOM_MS milliseconds
   * to go into effect.
   * @param token
   */
  public void setToken(String token) {
    if (!propertiesFileHelper.getToken().equals(token)) {
      propertiesFileHelper.putProperty(PropertyKeys.TOKEN, token);
      lb.refreshChannels(false);
    }
  }

  /**
   * Set urls to send to. See PropertyKeys.COLLECTOR_URI
   * for more information.
   * @param urls comma-separated list of urls
   */
  public void setUrls(String urls) {
    if (!propertiesFileHelper.urlsStringToList(urls).equals(
            propertiesFileHelper.getUrls())) {
      // a single url or a list of comma separated urls
      propertiesFileHelper.putProperty(PropertyKeys.COLLECTOR_URI, urls);
      lb.refreshChannels(true);
    }
  }

  /**
   * Use this method to change multiple settings on the connection.
   * See PropertyKeys class for more information.
   * @param props
   */
  public void setProperties(Properties props) {
    Properties diffs = propertiesFileHelper.getDiff(props);
    boolean refreshChannels = false;
    boolean dnsLookup = false;

    for (String key : diffs.stringPropertyNames()) {
      switch (key) {
        case PropertyKeys.ACK_TIMEOUT_MS:
          setAckTimeoutMS(Long.parseLong(diffs.getProperty(key)));
          break;
        case PropertyKeys.COLLECTOR_URI:
          propertiesFileHelper.putProperty(PropertyKeys.COLLECTOR_URI,
                  diffs.getProperty(key));
          dnsLookup = true;
          refreshChannels = true;
          break;
        case PropertyKeys.TOKEN:
          propertiesFileHelper.putProperty(PropertyKeys.TOKEN,
                  diffs.getProperty(key));
          refreshChannels = true;
          break;
        case PropertyKeys.HEC_ENDPOINT_TYPE:
          propertiesFileHelper.putProperty(PropertyKeys.HEC_ENDPOINT_TYPE,
                  diffs.getProperty(key));
          break;
        default:
          LOG.warn("Attempt to change property not supported: " + key);
      }
    }
    if (refreshChannels) {
      lb.refreshChannels(dnsLookup);
    }
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
  
  public List<EventBatch> getUnackedEvents(HecChannel c){
    return timeoutChecker.getUnackedEvents(c);
  } 
  
  public void release(Comparable id){
    //lb.getCheckpointManager().cancel(events);
  }
}
