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
 * Represents an illegal state in Cloudfwd that requires resolution by the client. Can be thrown either directly from 
 * the Thread that calls Connection methods, or can be passed to a Connection.failed callback.
 *
 *
 * @author ghendrey
 */

public class HecConnectionStateException extends IllegalStateException {
    
    public HecConnectionStateException(String s, Type t, Exception causedBy){
        super(s, causedBy);
        this.type = t;
    }

    public HecConnectionStateException(String s, Type t) {
        super(s);
        this.type = t;
    }
    

    public enum Type {
        /**
         * This type is only thrown if PropertyKeys.ENABLE_CHECKPOINT=true. Attempts to send EventBatch that is still pending acknowledgement.
        *//**
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
        CONFIGURATION_EXCEPTION,
        
        /**
         * When instantiating a connection, a channel was not able to get back a response to its preflight checks in a timely fashion.
         */
        CHANNEL_PREFLIGHT_TIMEOUT,
        /**
         * No channels existed in this connection
         */
        NO_HEC_CHANNELS,      
        /**
         * No ConnectionCallbacks provided
         */
        NO_CALLBACKS_PROVIDED        
    }
    private final Type type;

    /**
     * @return the type
     */
    public Type getType() {
        return type;
    }


    public String toString() {
        return super.toString() + type;
    }
}