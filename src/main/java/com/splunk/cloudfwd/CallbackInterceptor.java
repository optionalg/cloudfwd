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
import java.util.function.Consumer;

/**
 *
 * @author ghendrey
 */
class CallbackInterceptor implements FutureCallback {

  FutureCallback futureCallback;
  private final Consumer<EventBatch> wrappedAcknowledged;
  private final Consumer<EventBatch> before;

  public CallbackInterceptor(FutureCallback futureCallback,
          Consumer<EventBatch> before) {
    this.futureCallback = futureCallback;
    //it's possible to pre-compose the wrappedAcknowledged function
    this.wrappedAcknowledged = before.andThen(futureCallback::acknowledged);    
    this.before = before;
  }

  @Override
  public void acknowledged(EventBatch events) {
    this.wrappedAcknowledged.accept(events); //call the precompositon that wraps/intercepts futureCallback acknowledge
  }

  @Override
  public void failed(EventBatch events, Exception ex) {
    //since we cannot pre-compose these two actions, because of their different method signatures
    //we just call them in sequence
    this.before.accept(events);
    this.futureCallback.failed(events, ex);
  }

  @Override
  public void checkpoint(EventBatch events) {
    futureCallback.checkpoint(events); //we don't need to wrap checkpoint at present
  }

  FutureCallback unwrap() {
    return this.futureCallback;
  }

}
