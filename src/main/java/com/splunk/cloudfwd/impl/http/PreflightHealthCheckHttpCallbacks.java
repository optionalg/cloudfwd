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
import com.splunk.cloudfwd.impl.http.lifecycle.LifecycleEvent;
import static com.splunk.cloudfwd.impl.http.lifecycle.LifecycleEvent.Type.N2K_HEC_HEALTHY;
import static com.splunk.cloudfwd.impl.http.lifecycle.LifecycleEvent.Type.SPLUNK_IN_DETENTION;
import com.splunk.cloudfwd.impl.http.lifecycle.Response;
import java.io.IOException;
import org.slf4j.Logger;
import static com.splunk.cloudfwd.impl.http.lifecycle.LifecycleEvent.Type.UNHANDLED_NON_200;

/**
  Code    HTTP status	HTTP status code	Status message
    0	200	OK                                     Success                
    1	403	Forbidden                          Token disabled
    2	401	Unauthorized                     Token is required
    3	401	Unauthorized                     Invalid authorization
    4	403	Forbidden                          Invalid token
    5	400	Bad Request                       No data
    6	400	Bad Request                       Invalid data format
    7	400	Bad Request                       Incorrect index
    8	500	Internal Error	  Internal server error
    9	503	Service Unavailable	  Server is busy
    10	400	Bad Request                       Data channel is missing
    11	400	Bad Request                       Invalid data channel
    12	400	Bad Request                       Event field is required
    13	400	Bad Request	                     Event field cannot be blank
    14	400	Bad Request	                     ACK is disabled 
 * @author ghendrey
 */
class PreflightHealthCheckHttpCallbacks extends AbstractHttpCallback {

    private final HttpSender sender;
    private final Logger LOG;
    private final ObjectMapper mapper = new ObjectMapper();

    public PreflightHealthCheckHttpCallbacks(HttpSender sender) {
        super(sender.getConnection());
        this.sender = sender;
        this.LOG = sender.getConnection().getLogger(
                PreflightHealthCheckHttpCallbacks.class.
                getName());
    }

    @Override
    public void completed(String reply, int code) {
        try {
            handleResponse(code, reply);
        } catch (Exception ex) {
            LOG.error(
                    "failed to unmarshal server response in pre-flight health check {}",
                    reply);
            invokeFailedCallback(ex);
        }
    }

    private void handleResponse(int statusCode, String reply) throws IOException {
        LifecycleEvent.Type type;
        switch (statusCode) {
            case 404: //detention ... NOT one of the server responses that looks like {"text":"foo", "code":i}
                type = SPLUNK_IN_DETENTION;
                break;
            case 503:  //have to treat 503/busy same as N2K_HEC_HEALTHY else preflight can freeze channel
            case 200:
                LOG.info("HEC check is good");
                type = N2K_HEC_HEALTHY;
                break;
            default:
                type = handeCommonServerNon200s(reply, statusCode);
        }
        Response lifecycleEvent = new Response(type, statusCode, reply,
                sender.getBaseUrl());
        sender.getChannelMetrics().update(lifecycleEvent);
    }

    private LifecycleEvent.Type handeCommonServerNon200s(String reply,
            int statusCode) throws IOException {
        LifecycleEvent.Type type;
        HecErrorResponseValueObject r = mapper.readValue(reply,
                HecErrorResponseValueObject.class);
        type = NonBusyServerErrors.type(statusCode, reply);
        Exception e = new HecServerErrorResponseException(r.getText(),
                r.getCode(), sender.getBaseUrl());
        invokeFailedCallback(e);
        return type;
    }

    @Override
    public void failed(Exception ex) {
        LOG.error(
                "HEC pre-flight health check via /ack endpoint failed with exception {}",
                ex);
        invokeFailedCallback(ex);
    }

    @Override
    public void cancelled() {
        LOG.warn("HEC pre-flight health check cancelled");
        invokeFailedCallback(new Exception(
                "HEC pre-flight health check via /ack endpoint cancelled."));
    }

    //Hardened to catch exceptions that could come from the application's failed callback
    private void invokeFailedCallback(Exception ex) {
        try {
            LOG.error(
                    "HEC pre-flight health check via /ack endpoint failed with exception {}",
                    ex);
            sender.getConnection().getCallbacks().
                    failed(null, ex);
        } catch (Exception e) {
            //if the application's callback is throwing an exception we have no way to handle this, other
            //than log an error
            LOG.error(
                    "Caught exception in ConnectionCallbacks.failed for HEC pre-flight health check {}",
                    ex);
        }
    }

}
