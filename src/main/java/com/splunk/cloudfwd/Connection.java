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

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

/**
 *
 * @author ghendrey
 */
public class Connection implements Closeable {

  public final static long DEFAULT_SEND_TIMEOUT_MS = 60 * 1000;
  private final LoadBalancer lb;
  private final CallbackInterceptor callbacks;
  private final TimeoutChecker timeoutChecker;
  private boolean closed;

  public Connection(FutureCallback callbacks) {
    //when callbacks.acknowledged or callbacks.failed is called, in both cases we need to remove
    //the EventBatch that succeeded or failed from the timoutChecker
    this.timeoutChecker = new TimeoutChecker(DEFAULT_SEND_TIMEOUT_MS);
    this.callbacks = new CallbackInterceptor(callbacks,
            timeoutChecker::removeEvents);
    this.timeoutChecker.setInterceptor(this.callbacks);
    this.lb = new LoadBalancer(this);
  }

  public Connection(FutureCallback callbacks, Properties settings) {
    this.timeoutChecker = new TimeoutChecker(DEFAULT_SEND_TIMEOUT_MS);
    this.callbacks = new CallbackInterceptor(callbacks,
            timeoutChecker::removeEvents);
    this.timeoutChecker.setInterceptor(this.callbacks);
    this.lb = new LoadBalancer(this, settings);
  }

  public synchronized void setSendTimeout(long ms) {
    this.timeoutChecker.setTimeout(ms);
  }

  public long getSendTimeout() {
    return this.timeoutChecker.getTimeout();
  }

  @Override
  public void close() {
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

  public void sendBatch(EventBatch events) {
  if(closed){
      throw new IllegalStateException("Attempt to send on closed channel.");
    }
    timeoutChecker.start();
    timeoutChecker.add(events);
    lb.sendBatch(events);
  }

  /**
   * @return the callbacks
   */
  public FutureCallback getCallbacks() {
    return callbacks;
  }

  /**
   * @return the closed
   */
  public boolean isClosed() {
    return closed;
  }

}
