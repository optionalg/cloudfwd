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

import com.splunk.cloudfwd.http.EventBatch;
import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 *
 * @author ghendrey
 */
public class Connection implements Closeable {

  public final static long SEND_TIMEOUT = 60 * 1000; //FIXME TODO make configurable
  LoadBalancer lb;
  private final FutureCallback callbacks;

  public Connection(FutureCallback callbacks) {
    this.callbacks = callbacks;
    this.lb = new LoadBalancer(this);
  }

  public Connection(FutureCallback callbacks, Properties settings) {
    this.callbacks = callbacks;
    this.lb = new LoadBalancer(this, settings);
  }

  @Override
  public void close() {
    //we must close asynchronously to prevent deadlocking
    //when close() is invoked from a callback like the
    //Exception handler
    new Thread(() -> {
      lb.close();
    },  "LoadBalancer Closer").start();
  }

  public void closeNow() {
    //we must close asynchronously to prevent deadlocking
    //when close() is invoked from a callback like the
    //Exception handler
    new Thread(() -> {
      lb.closeNow();
    }, "LoadBalancer Closer").start();
  }

  public void sendBatch(EventBatch events) throws TimeoutException {
    lb.sendBatch(events);
  }

  /**
   * @return the callbacks
   */
  public FutureCallback getCallbacks() {
    return callbacks;
  }

}
