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

import com.splunk.cloudfwd.impl.EventBatchImpl;

/**
 *
 * @author ghendrey
 */
public interface Connection {

    void close();

    void closeNow();

    void flush() throws HecConnectionTimeoutException;

    /**
     * @return the callbacks
     */
    ConnectionCallbacks getCallbacks();

    /**
     * @return the closed
     */
    boolean isClosed();

    void release(Comparable id);

    /**
     * sendBatch will immediately send the EventBatchImpl, returning the number of bytes sent, or throws an
 HecConnectionTimeoutException if BLOCKING_TIMEOUT_MS have expired before the batch could be sent.
     * HecIllegalStateException can be thrown if the connection has already acknowledged an EventBatchImpl with the same id,
 or if an EventBatchImpl with the same id has already previously been sent.
     * @param events
     * @return
     * @throws HecConnectionTimeoutException
     */
    int sendBatch(EventBatchImpl events) throws HecConnectionTimeoutException;
    
    /**
     * Returns a live ConnectionsSettings instance that can be used to change the behavior of the connection while
     * it is in use.
     * @return ConnectionSettings
     */
    public ConnectionSettings getSettings();
    
}
