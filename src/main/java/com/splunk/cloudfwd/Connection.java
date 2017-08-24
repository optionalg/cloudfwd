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

import com.splunk.cloudfwd.util.CallbackInterceptor;
import com.splunk.cloudfwd.util.LoadBalancer;
import com.splunk.cloudfwd.util.PropertiesFileHelper;
import com.splunk.cloudfwd.util.TimeoutChecker;
import java.io.Closeable;
import java.util.Properties;
import java.util.UUID;

/**
 * Represents a reliable Connection to either the "event" HEC endpoint or the "raw" HEc endpoint.
 * @author ghendrey
 */
public class Connection implements Closeable {

  public final static long DEFAULT_SEND_TIMEOUT_MS = 5 * 60 * 1000;

  /**
   * Used to select either structured  HEC /event endpoint, or raw HEC endpoint
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
  private int charBufferSize;

  /* *********************** METRICS ************************ */
  private String testName;
  private String runId;
  private String testId;
  /* *********************** /METRICS ************************ */

  public Connection(ConnectonCallbacks callbacks) {
    init(callbacks);
    this.lb = new LoadBalancer(this);
  }

  public Connection(ConnectonCallbacks callbacks, Properties settings) {
    init(callbacks);
    this.lb = new LoadBalancer(this, settings);
  }

  private void init(ConnectonCallbacks callbacks) {
    this.events = new EventBatch();
    this.hecEndpointType = HecEndpoint.RAW_EVENTS_ENDPOINT;
    //when callbacks.acknowledged or callbacks.failed is called, in both cases we need to remove
    //the EventBatch that succeeded or failed from the timoutChecker
    this.timeoutChecker = new TimeoutChecker(DEFAULT_SEND_TIMEOUT_MS);
    this.callbacks = new CallbackInterceptor(callbacks,
            timeoutChecker::removeEvents);
    this.timeoutChecker.setInterceptor(this.callbacks);
  }

  public synchronized void setSendTimeout(long ms) {
    this.timeoutChecker.setTimeout(ms);
  }

  public long getSendTimeout() {
    return this.timeoutChecker.getTimeout();
  }

  @Override
  public void close() {
    flush();
    this.closed = true;
    //we must close asynchronously to prevent deadlocking
    //when close() is invoked from a callback like the
    //Exception handler
    new Thread(() -> {
      lb.close();
      timeoutChecker.stop();
    }, "Connection Closer").start();
  }

  public void closeNow() {
    this.closed = true;
    //we must close asynchronously to prevent deadlocking
    //when closeNow() is invoked from a callback like the
    //Exception handler
    new Thread(() -> {
      lb.closeNow();
      timeoutChecker.stop();
    }, "Connection Closer").start();
  }

  public synchronized void send(Event event) {
    if (null == this.events) {
      this.events = new EventBatch();
    }
    this.events.add(event);
    if (this.events.isFlushable(charBufferSize)) {
      sendBatch(events);
    }

  }

  public synchronized void sendBatch(EventBatch events) {
    if (closed) {
      throw new IllegalStateException("Attempt to send on closed channel.");
    }
    timeoutChecker.start();
    timeoutChecker.add(events);
    lb.sendBatch(events);
    this.events = null; //batch is in flight, null it out
  }

  public synchronized void flush() {
    if(null != events){
      sendBatch(events);
    }
  }

  /**
   * @return the callbacks
   */
  public ConnectonCallbacks getCallbacks() {
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
   * @return the charBufferSize
   */
  public int getCharBufferSize() {
    return charBufferSize;
  }

  /**
   * @param charBufferSize the charBufferSize to set
   */
  public void setCharBufferSize(int charBufferSize) {
    this.charBufferSize = charBufferSize;
  }

  public PropertiesFileHelper getPropertiesFileHelper() {
    return lb.getPropertiesFileHelper();
  }

  public String getRunId() {
    return runId;
  }

  public void setRunId(String runId) {
    this.runId = runId;
  }

  public String getTestId() {
    return testId;
  }

  public void setTestId(String testId) {
    this.testId = testId;
  }

  public String getTestName() {
    return testName;
  }

  public void setTestName(String testName) {
    this.testName = testName;
  }
}
