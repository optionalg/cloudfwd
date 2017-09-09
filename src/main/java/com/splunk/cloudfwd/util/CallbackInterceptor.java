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
package com.splunk.cloudfwd.util;

import com.splunk.cloudfwd.ConnectionCallbacks;
import com.splunk.cloudfwd.EventBatch;
import java.util.function.Consumer;

/**
 * Server EventTrackers keep track of EventBatches by their ids. When an EventBatch fails, the
 * EventTrackers must be canceled. When an EventBatch is acknowledged, also the EventTrackers must 
 * be canceled, because in either case the EventBatch is no longer tracked by the Connection.
 * @author ghendrey
 */
public class CallbackInterceptor implements ConnectionCallbacks {

  ConnectionCallbacks callbacks;

  public CallbackInterceptor(ConnectionCallbacks callbacks) {
    this.callbacks = callbacks;
  }

  @Override
  public void acknowledged(EventBatch events) {
    callbacks.acknowledged(events);
    events.cancelEventTrackers(); //we can't cancel the 
  }

  @Override
  public void failed(EventBatch events, Exception ex) {
    this.callbacks.failed(events, ex);
    if(null != events){
      events.cancelEventTrackers();
    }    
  }

  @Override
  public void checkpoint(EventBatch events) {
    callbacks.checkpoint(events); //we don't need to wrap checkpoint at present
  }

  public ConnectionCallbacks unwrap() {
    return this.callbacks;
  }

}
