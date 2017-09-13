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

/**
 *
 * @author ghendrey
 */
public abstract class LifecycleEvent {

  public enum Type {
	// States tied to an EventBatch object
    EVENT_BATCH_BORN,
    PRE_EVENT_POST,
    EVENT_POSTED,
    EVENT_POST_NOT_OK,
    EVENT_POST_FAILURE,
    EVENT_POST_OK,
    PRE_ACK_POLL,
    ACK_POLL_OK,
    ACK_POLL_NOT_OK,
    ACK_POLL_FAILURE,

    // States without an EventBatch object
    HEALTH_POLL_OK,
    HEALTH_POLL_INDEXER_BUSY,
    HEALTH_POLL_FAILED,
    HEALTH_POLL_ERROR,
    PREFLIGHT_CHECK_FAILED,
    PREFLIGHT_CHECK_OK,
    PREFLIGHT_CHECK_NOT_OK
  };

  private final Type type;

  public LifecycleEvent(final Type type) {
    this.type = type;
  }

  /**
   * @return the type
   */
  public Type getType() {
    return type;
  }

  @Override
  public String toString() {
    return "LifecycleEvent{" + "type=" + type + '}';
  }
  
  


}
