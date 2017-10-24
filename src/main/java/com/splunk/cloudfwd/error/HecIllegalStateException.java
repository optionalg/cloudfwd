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
    /**
     * This type gets thrown after after an unacknowledged EventBatch when
     */
    EVENT_NOT_ACKNOWLEDGED_BUT_HIGHWATER_RECOMPUTED,
    /**
     * This type gets thrown after an attempt to send RoundRobin occurs, but there is no channel available.
     */
    LOAD_BALANCER_NO_CHANNELS,
    /**
     * This type gets thrown when ackID does not match RecordedAckID.
     */
    ACK_ID_MISMATCH,
    /**
     * This type gets thrown when EventTracker is already registered on EventBatch.
     */
    EVENT_TRACKER_ALREADY_REGISTERED,
    /**
     * This type gets thrown when the channel is null in HTTPSender.
     */
    NULL_CHANNEL,
    /**
     * This type gets thrown when there is an incorrect event type object in RawEvent.
     */
    INCORRECT_EVENT_TYPE_OBJECT,
    /**
     * This type gets thrown when the sender's channel ID does not match.
     */
    CHANNEL_MISMATCH,
    /**
     * This type gets thrown when EventBatch has no target endpoint.
     */
    NO_TARGET,
    /**
     * This type gets thrown when AckPollController is already started in IndexDiscoveryScheduler.
     */
    ALREADY_POLLING_ACKS,
    /**
     * This type gets thrown when CorePoolSize's private variable is set to 0.
     */
    CORE_POOL_SIZE_ZERO,
    /**
     * This type gets thrown when un-acked count on a channel is an illegal negative value.
     */
    NEGATIVE_UNACKED_COUNT,
    /**
     * This type gets thrown when there is an attempts to remove non-empty channels that contain un-acked payloads.
     */
    REMOVE_NON_EMPTY_CHANNEL,
    /**
     * This type gets thrown when InputStream experiences an error in loading cloudfwd.properties resources (example: wrong file name).
     */
    CANNOT_LOAD_PROPERTIES,
    /**
     * This type gets thrown if a resend is attempted when the resend queue poller thread is not running.
     */
    RESEND_ATTEMPTED_ON_INACTIVE_RESEND_QUEUE
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