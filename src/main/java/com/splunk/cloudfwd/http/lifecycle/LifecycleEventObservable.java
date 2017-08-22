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
package com.splunk.cloudfwd.http.lifecycle;

import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.FutureCallback;
import com.splunk.cloudfwd.EventBatch;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author ghendrey
 */
public class LifecycleEventObservable {

  private static final Logger LOG = Logger.getLogger(
          LifecycleEventObservable.class.getName());
  private final List<LifecycleEventObserver> observers = new ArrayList<>();
  protected final Connection connection;

  public LifecycleEventObservable(Connection connection) {
    this.connection = connection;
  }

  
  
  public void addObserver(LifecycleEventObserver o) {
    this.observers.add(o);
  }

  protected final synchronized void notifyObservers(LifecycleEvent event) {
    try {
      observers.forEach((LifecycleEventObserver observer) -> {
        observer.update(event);
      });
    } catch (Exception ex) {
      LOG.log(Level.SEVERE, ex.getMessage(), ex);
      FutureCallback c = connection.getCallbacks();
      final EventBatch events = (ex instanceof EventBatchLifecycleEvent) ? ((EventBatchLifecycleEvent) ex).
              getEvents() : null;
      //new Thread(() -> {//FIXME TODO - usea thread pool
        c.failed(events, ex); //FIXME TODO -- there are many places where we should be calling failed. 
      //}).start();
    }

  }

}
