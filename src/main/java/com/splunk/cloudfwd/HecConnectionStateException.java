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

/**
 * Represents an illegal state in Cloudfwd that requires resolution by the client.
 *
 *
 * @author ghendrey
 */

public class HecConnectionStateException extends IllegalStateException {

    public enum Type {
        /**
         * This type is only thrown if PropertyKeys.ENABLE_CHECKPOINT=true. Attempts to send EventBatch that is still pending acknowledgement.
        */
        ALREADY_SENT,
        /**
         * This type is only thrown if PropertyKeys.ENABLE_CHECKPOINT=true. EventBatch is already acknowledged and the EventBatchID is smaller than CheckpointID.
         */
        ALREADY_ACKNOWLEDGED,
        /**
         * This type is thrown when EventBatch contains events whose type is incompatible with the HEC endpoint.
         */
        WRONG_EVENT_FORMAT_FOR_ENDPOINT,
        /**
         * This type is thrown when there is an Illegal attempt to add an event to an already flushed EventBatch. EventBatch is not reusable.
         */
        ALREADY_FLUSHED,
        /**
         * This type is thrown when we try to send events but the required callback functions are not set.
         */
        CONNECTION_CALLBACK_NOT_SET,
        /**
         * This type is thrown when there is an attempt to sendBatch() on a closed connection. Once a Connection is closed, it cannot be re-opened.
         */
        SEND_ON_CLOSED_CONNECTION,
        /**
         * This type is thrown when there is a missing required property or invalid property value in cloudfwd.properties or when Connection constructor overrides object.
         */
        CONFIGURATION_EXCEPTION
    }
    private final Type type;

    /**
     * @return the type
     */
    public Type getType() {
        return type;
    }

    public HecConnectionStateException(String s, Type t) {
        super(s);
        this.type = t;
    }

    public String toString() {
        return super.toString() + type;
    }
}