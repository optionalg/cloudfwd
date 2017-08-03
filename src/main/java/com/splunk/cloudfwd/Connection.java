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
public class Connection implements Closeable{
  public final static long SEND_TIMEOUT = 5 * 1000; //FIXME TODO make configurable
  LoadBalancer lb;
  private Consumer<Exception> exceptionHandler;
  
  public Connection(){
    this.lb = new LoadBalancer(this);
  }
  
  public Connection(Properties settings){
    this.lb = new LoadBalancer(this, settings);
  }

  @Override
  public void close()  {
    //we must close asynchronously to prevent deadlocking
    //when close() is invoked from a callback like the
    //Exception handler
    new Thread(() -> {
      lb.close();
    }).start();
  }
  
  
  public void sendBatch(EventBatch events, Runnable callback) throws TimeoutException {
    lb.sendBatch(events, callback);
  }
  
  public void setExceptionHandler(Consumer<Exception> handler){
    this.exceptionHandler = handler;
  }

  /**
   * @return the exceptionHandler
   */
  public Consumer<Exception> getExceptionHandler() {
    return exceptionHandler;
  }
  
}
