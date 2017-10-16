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
package com.splunk.cloudfwd.impl.http.httpascync;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.splunk.cloudfwd.error.HecConnectionStateException;
import static com.splunk.cloudfwd.error.HecConnectionStateException.Type.CONFIGURATION_EXCEPTION;
import com.splunk.cloudfwd.error.HecServerBusyException;
import com.splunk.cloudfwd.error.HecServerErrorResponseException;
import com.splunk.cloudfwd.impl.EventBatchImpl;
import com.splunk.cloudfwd.LifecycleEvent;
import com.splunk.cloudfwd.impl.http.EventPostResponseValueObject;
import com.splunk.cloudfwd.impl.http.HecIOManager;
import com.splunk.cloudfwd.impl.http.HttpSender;
import static com.splunk.cloudfwd.LifecycleEvent.Type.*;

import com.splunk.cloudfwd.impl.http.ServerErrors;
import com.splunk.cloudfwd.impl.http.lifecycle.Response;
import org.slf4j.Logger;

import java.io.IOException;

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
public class HttpCallbacksEventPost extends HttpCallbacksAbstract {

    private final Logger LOG;
    private final EventBatchImpl events;
    private final ObjectMapper mapper = new ObjectMapper();

    public HttpCallbacksEventPost(HecIOManager m,
            EventBatchImpl events) {
        super(m, "event_post");
        this.events = events;
        LOG = getConnection().getLogger(HttpCallbacksEventPost.class.getName());
    }

    @Override
    public void completed(String reply, int code) {
        try {
            switch (code) {
                case 200:
                    consumeEventPostOkResponse(reply, code);
                    break;
                case 503:    
                    warn(reply, code);
                    notifyBusyAndResend(reply, code, EVENT_POST_INDEXER_BUSY);
                    break;
                case 504:                 
                    warn(reply, code);
                    notifyBusyAndResend(reply, code, EVENT_POST_GATEWAY_TIMEOUT);
                    break;                    
                default:
                    handleServerError(reply, code);
                    notify(EVENT_POST_NOT_OK, code, reply, events);
            }
        } catch (Exception e) {
            invokeFailedEventsCallback(events, e);
        }
    } //end completed()    

    @Override
    public void failed(Exception ex) {
        notifyFailedAndResend(ex);
    }

    @Override
    public void cancelled() {
        try {
            LOG.error("Event post cancelled on channel  {}, event batch {}",
                    getSender().getChannel(), events);
            Exception ex = new RuntimeException(
                    "HTTP post cancelled while posting events  " + events);
            notifyFailed(EVENT_POST_FAILED, events, ex);
            invokeFailedEventsCallback(events, ex);
        } catch (Exception e) {
            invokeFailedEventsCallback(events, e);
        }
    } //end cancelled()    


    private void handleServerError(String reply, int statusCode) throws IOException {
        HecServerErrorResponseException ex = ServerErrors.toErrorException(reply, statusCode, getBaseUrl());
        if (ex.getErrorType() == HecServerErrorResponseException.Type.RECOVERABLE_CONFIG_ERROR) {
            invokeFailedEventsCallback(events, ex); // fail fast on configuration errors
        } else {
            resend(ex); // try resending (e.g. Splunk server might be rebooting)
        }
    }

    private void resend(Exception ex) {
        Runnable r = () -> {
            try {
                events.addSendException(ex);
                LOG.warn("resending events through load balancer {} on channel {}",
                        events, getSender().getChannel());
                getSender().getConnection().getLoadBalancer().
                        sendRoundRobin(events, true); //will callback failed if max retries exceeded
            } catch (Exception e) {
                invokeFailedEventsCallback(events, e); //includes HecMaxRetriesException
            }
        };
        // Need to do this in a new thread: if resending happens in this thread and ends up spinning in the load balancer,
        // it'll prevent other threads from being able to send on the http client, since this thread holds onto a
        // lock in the http client until the callback finishes
        new Thread(r, "event post callbacks resender for channel " + getChannel()).start();
    }

    private void notifyFailedAndResend(Exception ex) {
        LOG.error("channel {} failed to post event batch {}",
            getChannel(), events);
        notifyFailed(EVENT_POST_FAILED, events, ex);
        resend(ex);
    }

    private void notifyBusyAndResend(String reply, int code, LifecycleEvent.Type t) {
        Response r = new Response(t,code, reply, getSender().getBaseUrl());
        notify(r);
        resend(new HecServerBusyException(reply));
    }
      
    public void consumeEventPostOkResponse(String resp, int httpCode) throws Exception {
        LOG.debug("{} Event post response: {}", getChannel(), resp);
        EventPostResponseValueObject epr = mapper.readValue(resp,
                EventPostResponseValueObject.class);
        if (epr.isAckIdReceived()) {
            events.setAckId(epr.getAckId()); //tell the batch what its HEC-generated ackId is.
        } else if (epr.isAckDisabled()) {
            throwConfigurationException(getSender(), httpCode, resp);
        }

        getSender().getAcknowledgementTracker().handleEventPostResponse(epr, events);

        // start polling for acks
        getManager().startAckPolling();

        notify(EVENT_POST_OK, 200, resp, events);
    }

    private void throwConfigurationException(HttpSender sender, int httpCode, String resp) 
            throws HecConnectionStateException {
        notify(EVENT_POST_ACKS_DISABLED,httpCode, resp, events);
        throw new HecConnectionStateException(
                "Event POST responded without ackId (acknowledgements are disabled on HEC endpoint).",
                CONFIGURATION_EXCEPTION);
    }

} //end HecHttpCallbacks
