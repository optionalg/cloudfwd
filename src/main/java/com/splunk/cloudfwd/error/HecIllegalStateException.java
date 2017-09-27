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
package com.splunk.cloudfwd.error;

/**
 * Represents an internal illegal state in Cloudfwd, which is not recoverable but should be logged.
 *
 * @author ghendrey
 */
public class HecIllegalStateException extends IllegalStateException {

  public enum Type {
    EVENT_NOT_ACKNOWLEDGED_BUT_HIGHWATER_RECOMPUTED,
    LOAD_BALANCER_NO_CHANNELS,
    ACK_ID_MISMATCH,
    EVENT_TRACKER_ALREADY_REGISTERED,
    NULL_CHANNEL,
    INCORRECT_EVENT_TYPE_OBJECT,
    CHANNEL_MISMATCH,
    NO_TARGET,
    ALREADY_POLLING_ACKS,
    CORE_POOL_SIZE_ZERO,
    NEGATIVE_UNACKED_COUNT,
    REMOVE_NON_EMPTY_CHANNEL,
    CANNOT_LOAD_PROPERTIES
  }
  private final Type type;

  /**
   * @return the type
   */
  public Type getType() {
    return type;
  }

  public HecIllegalStateException(String s, Type t) {
    super(s);
    this.type = t;
  }

  public String toString() {
    return super.toString() + type;
  }

}