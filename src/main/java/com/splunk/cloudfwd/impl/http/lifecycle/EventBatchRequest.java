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

import com.splunk.cloudfwd.LifecycleEvent;
import com.splunk.cloudfwd.impl.EventBatchImpl;

/**
 *
 * @author ghendrey
 */
public class EventBatchRequest extends LifecycleEvent implements EventBatchLifecycleEvent{

  private final EventBatchImpl eventBatch;
  
  public EventBatchRequest(Type type, EventBatchImpl events) {
    super(type);
    this.eventBatch = events;
    events.setState(type);
  }

    @Override
    public String toString() {
        return "EventBatchRequest{" + super.toString()+" eventBatch=" + eventBatch + '}';
    }
  
  

  /**
   * @return the eventBatch
   */
  @Override
  public EventBatchImpl getEvents() {
    return eventBatch;
  }

    @Override
    public Exception getException() {
        return null;
    }
  
}
