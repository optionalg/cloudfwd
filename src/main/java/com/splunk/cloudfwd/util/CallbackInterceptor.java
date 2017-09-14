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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Server EventTrackers keep track of EventBatches by their ids. When an
 * EventBatch fails, the EventTrackers must be canceled. When an EventBatch is
 * acknowledged, also the EventTrackers must be canceled, because in either case
 * the EventBatch is no longer tracked by the Connection.
 *
 * @author ghendrey
 */
public class CallbackInterceptor implements ConnectionCallbacks {
    private static final Logger LOG = LoggerFactory.getLogger(CallbackInterceptor.class);

    ConnectionCallbacks callbacks;

    public CallbackInterceptor(ConnectionCallbacks callbacks) {
        this.callbacks = callbacks;
    }       

    @Override
    public void acknowledged(EventBatch events) {
        try {
            callbacks.acknowledged(events);
        } catch (Exception e) {
            LOG.error("Caught exception from ConnectionCallbacks.acknowledged: " + e.getMessage());
            LOG.error(e.getMessage(), e);
        } finally {
            events.cancelEventTrackers(); //remove the EventBatch from the places in the system it should be removed
        }
    }

    @Override
    public void failed(EventBatch events, Exception ex) {
        try {
            this.callbacks.failed(events, ex);
        } catch (Exception e) {
            LOG.error("Caught exception from ConnectionCallbacks.failed: " + e.getMessage());
            LOG.error(e.getMessage(), e);
        } finally {
            if (null != events) {
                events.cancelEventTrackers();//remove the EventBatch from the places in the system it should be removed
            }
        }
    }

    @Override
    public void checkpoint(EventBatch events) {
        try {
            callbacks.checkpoint(events); //we don't need to wrap checkpoint at present
        } catch (Exception e) {
            LOG.error("Caught exception from ConnectionCallbacks.checkpoint: " + e.getMessage());
            LOG.error(e.getMessage(), e);
        }
    }

    public ConnectionCallbacks unwrap() {
        return this.callbacks;
    }

}
