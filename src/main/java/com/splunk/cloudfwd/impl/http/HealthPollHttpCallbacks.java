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
package com.splunk.cloudfwd.impl.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.splunk.cloudfwd.HecServerErrorResponseException;
import com.splunk.cloudfwd.impl.ConnectionImpl;
import com.splunk.cloudfwd.impl.http.lifecycle.LifecycleEvent;
import static com.splunk.cloudfwd.impl.http.lifecycle.LifecycleEvent.Type.HEALTH_POLL_INDEXER_BUSY;
import static com.splunk.cloudfwd.impl.http.lifecycle.LifecycleEvent.Type.HEALTH_POLL_OK;
import com.splunk.cloudfwd.impl.http.lifecycle.RequestFailed;
import com.splunk.cloudfwd.impl.http.lifecycle.Response;
import java.io.IOException;
import java.util.logging.Level;
import org.slf4j.Logger;

/**
 *
 * @author ghendrey
 */
class HealthPollHttpCallbacks extends AbstractHttpCallback {

    private static final ObjectMapper mapper = new ObjectMapper();
    private final Logger LOG;
    private final HecIOManager manager;

    public HealthPollHttpCallbacks(final HecIOManager m) {
        super(m.getSender().getConnection());
        this.manager = m;
        this.LOG = m.getSender().getConnection().getLogger(
                HealthPollHttpCallbacks.class.getName());
    }

    @Override
    public void failed(Exception ex) {
        try {
            HttpSender sender = manager.getSender();
            LOG.error("Channel {} failed to poll health because {}",
                    sender.getChannel(), ex);
            sender.getChannelMetrics().
                    update(new RequestFailed(
                            LifecycleEvent.Type.HEALTH_POLL_FAILED,
                            ex));
        } catch (Exception e) {
            invokeFailedCallback(null, ex);
        }
    }

    @Override
    public void cancelled() {
        try {
            HttpSender sender = manager.getSender();
            Exception ex = new RuntimeException(
                    "HTTP post cancelled while polling for health on channel " + sender.
                    getChannel());
            sender.getChannelMetrics().
                    update(new RequestFailed(
                            LifecycleEvent.Type.HEALTH_POLL_FAILED,
                            ex));
        } catch (Exception ex) {
            invokeFailedCallback(null, ex);
        }
    }

    @Override
    public void completed(String reply, int code) {
        try {
            handleHealthPollResponse(code, reply);
        } catch (IOException ex) {
            invokeFailedCallback(reply, ex);
        }
    }

    private void handleHealthPollResponse(int statusCode, String reply) throws IOException {
        HttpSender sender = manager.getSender();
        LifecycleEvent.Type type;
        switch (statusCode) {
            case 200:
                LOG.info("Health check is good");
                type = HEALTH_POLL_OK;
                break;
            case 503:
                type = HEALTH_POLL_INDEXER_BUSY;
                break;
            default:
                type = handleServerErrorResponse(reply, statusCode, sender);
        }
        Response lifecycleEvent = new Response(type, statusCode, reply,
                sender.getBaseUrl());
        sender.getChannelMetrics().update(lifecycleEvent);
    }

    private LifecycleEvent.Type handleServerErrorResponse(String reply,
            int statusCode, HttpSender sender) throws IOException {
        LifecycleEvent.Type type;
        HecErrorResponseValueObject r = mapper.readValue(reply,
                HecErrorResponseValueObject.class);
        type = NonBusyServerErrors.type(statusCode, reply);
        Exception e = new HecServerErrorResponseException(r.getText(),
                r.getCode(), sender.getBaseUrl());
        invokeFailedCallback(reply, e);
        return type;
    }

    //Hardened to catch exceptions that could come from the application's failed callback
    private void invokeFailedCallback(String reply, Exception ex) {
        try {
            LOG.error(
                    "Server reply was {}. Health poll failed with exception {}",
                    ex);
            manager.getSender().getConnection().getCallbacks().
                    failed(null, ex);
        } catch (Exception e) {
            //if the application's callback is throwing an exception we have no way to handle this, other
            //than log an error
            LOG.error(
                    "Caught exception in ConnectionCallbacks.failed for  health check {}",
                    ex);
        }
    }

}
