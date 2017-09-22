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
package com.splunk.cloudfwd.impl.http.lifecycle;

import com.splunk.cloudfwd.impl.EventBatchImpl;
import com.splunk.cloudfwd.impl.ConnectionImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.splunk.cloudfwd.ConnectionCallbacks;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 *
 * @author ghendrey
 */
public class LifecycleEventObservable {
  private final Logger LOG;
  private final Collection<LifecycleEventObserver> observers = new ConcurrentLinkedQueue<>();
  protected final ConnectionImpl connection;

  public LifecycleEventObservable(ConnectionImpl connection) {
    this.connection = connection;
    this.LOG = connection.getLogger(LifecycleEventObservable.class.getName());
  }

  public void addObserver(LifecycleEventObserver o) {
    this.observers.add(o);
  }
  
  public void removeObserver(LifecycleEventObserver o){
    this.observers.remove(o);
  }

  protected final void notifyObservers(LifecycleEvent event) {
    try {
      observers.forEach((LifecycleEventObserver observer) -> {
        observer.update(event);
      });
    } catch (Exception ex) {
      LOG.error(ex.getMessage(), ex);
      ConnectionCallbacks c = connection.getCallbacks();
      final EventBatchImpl events = (ex instanceof EventBatchLifecycleEvent) ? ((EventBatchLifecycleEvent) ex).
              getEvents() : null;
      //new Thread(() -> {//FIXME TODO - usea thread pool
        c.failed(events, ex); //FIXME TODO -- there are many places where we should be calling failed.
      //}).start();
    }

  }

}
