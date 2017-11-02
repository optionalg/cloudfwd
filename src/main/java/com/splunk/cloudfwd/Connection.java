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

import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import com.splunk.cloudfwd.error.HecNoValidChannelsException;
import java.io.Closeable;
import java.util.Collection;
import java.util.List;


/**
 *
 * @author ghendrey
 */
public interface Connection extends Closeable{

    /**
     * The send method will send the Event immediately unless buffering is enabled. Buffering is
    enabled via either the setEventBatchSize method, or the EVENT_BATCH_SIZE property key. The buffer
    is flushed either by closing the Connection, calling flush, or calling send until EVENT_BATCH_SIZE bytes
    have accumulated in the Connections internal EventBatchImpl. When an EventBatchImpl is flushed, the connection's
    ConnectionCallbacks will be invoked, asynchronously. The send method may block for up to BLOCKING_TIMEOUT_MS
    milliseconds before throwing  an HecConnecionTimeoutException.
     * @param event
     * @return the number of bytes sent (will be zero unless buffer reaches EVENT_BATCH_SIZE and flushes)
     * @throws HecConnectionTimeoutException
     * @see com.splunk.cloudfwd.PropertyKeys
     */
    int send(Event event) throws HecConnectionTimeoutException, HecNoValidChannelsException;


    /**
     * Used to select either structured HEC /event endpoint or /raw HEC endpoint
     */
    public static enum HecEndpoint {
        STRUCTURED_EVENTS_ENDPOINT, RAW_EVENTS_ENDPOINT
    };

    /**
     * Non-blocking close method that asynchronously waits for PropertyKeys.CHANNEL_QUIESCE_TIMEOUT_MS 
     * for event batches to be acknowledged. After the timeout, calls the failed callback for each unacknowledged 
     * event batch. Cleans up all resources associated with the Connection object. Once a Connection object is closed, 
     * events cannot be sent through it and the same Connection object cannot be reopened. 
     * @see com.splunk.cloudfwd.PropertyKeys.CHANNEL_QUIESCE_TIMEOUT_MS
     */
    void close();
    
    /**
     * Blocking close method that waits for outstanding event batches to be acknowledged for a specified time. 
     * After the timeout, calls the failed callback for each unacknowledged event batch and returns them all in a 
     * Collection. Cleans up all resources associated with the Connection object. Once a Connection object is closed, 
     * events cannot be sent through it and the same Connection object cannot be reopened. 
     * @param timeoutMS time to wait, in milliseconds, until failing and returning all unacknowledged messages 
     * @return a Collection of event batches that were not acknowledged after timeoutMS
     */
    Collection<EventBatch> close(long timeoutMS);

    /**
     * Non-blocking immediate close method that does not wait for event batches to be acknowledged and asynchronously 
     * calls the failed callback for each unacknowledged event batch. Cleans up all resources associated with the 
     * Connection object. Once a Connection object is closed, events cannot be sent through it and the same Connection 
     * object cannot be reopened. 
     */
    void closeNow();

    void flush() throws HecConnectionTimeoutException, HecNoValidChannelsException;

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
     * sendBatch will immediately send the EventBatchImpl, returning the number
     * of bytes sent, or throws an HecConnectionTimeoutException if
     * BLOCKING_TIMEOUT_MS have expired before the batch could be sent.
     * HecIllegalStateException can be thrown if the connection has already
     * acknowledged an EventBatchImpl with the same id, or if an EventBatchImpl
     * with the same id has already previously been sent.
     *
     * @param events
     * @return
     * @throws HecConnectionTimeoutException
     */
    int sendBatch(EventBatch events) throws HecConnectionTimeoutException, HecNoValidChannelsException;

    /**
     * Returns a live ConnectionsSettings instance that can be used to change
     * the behavior of the connection while it is in use.
     *
     * @return ConnectionSettings
     */
    public ConnectionSettings getSettings();

    /**
     * Allows for replacement of the internal LoggerFactory with a customize one.
     * @param f
     */
    public void setLoggerFactory(HecLoggerFactory f);
    
     /**
     * healthCheck will retrieve health for each channel or trigger a health check if no channels have been made    
     */
    public  List<HecHealth> getHealth();
}
