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
import com.splunk.cloudfwd.EventBatch;
import com.splunk.cloudfwd.HecConnectionStateException;
import static com.splunk.cloudfwd.HecConnectionStateException.Type.CONFIGURATION_EXCEPTION;
import com.splunk.cloudfwd.HecServerBusyException;
import com.splunk.cloudfwd.HecServerErrorResponseException;
import com.splunk.cloudfwd.impl.EventBatchImpl;
import com.splunk.cloudfwd.impl.http.lifecycle.EventBatchFailure;
import com.splunk.cloudfwd.impl.http.lifecycle.EventBatchResponse;
import com.splunk.cloudfwd.LifecycleEvent;
import static com.splunk.cloudfwd.LifecycleEvent.Type.EVENT_POST_ACKS_DISABLED;
import static com.splunk.cloudfwd.LifecycleEvent.Type.EVENT_POST_NOT_OK;
import com.splunk.cloudfwd.impl.http.lifecycle.Response;
import java.io.IOException;
import org.slf4j.Logger;

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
class EventPostHttpCallbacks extends AbstractHttpCallback {

    private final Logger LOG;
    private final EventBatchImpl events;
    private final ObjectMapper mapper = new ObjectMapper();

    public EventPostHttpCallbacks(HecIOManager m,
            EventBatchImpl events) {
        super(m);
        this.events = events;
        LOG = m.getSender().getConnection().getLogger(
                EventPostHttpCallbacks.class.getName());
    }

    @Override
    public void completed(String reply, int code) {
        try {
            HttpSender sender = manager.getSender();
            switch (code) {
                case 200:
                    consumeEventPostOkResponse(reply, code);
                    break;
                case 503:
                    markBusyAndResend(reply, code);
                    break;
                default:
                     invokeFailedWithHecServerResponseException(reply, code, sender);
                     sender.getChannelMetrics(). update(
                             new EventBatchResponse(EVENT_POST_NOT_OK,code, reply, events, sender.getBaseUrl()));
            }
        } catch (Exception e) {
            invokeFailedCallback(events, e);
        }
    } //end completed()    

    @Override
    public void failed(Exception ex) {
        resend(ex);
    }

    @Override
    public void cancelled() {
        HttpSender sender = manager.getSender();
        try {
            LOG.error("Event post cancelled on channel  {}, event batch {}",
                    sender.getChannel(), events);
            Exception ex = new RuntimeException(
                    "HTTP post cancelled while posting events  " + events);
            sender.getChannelMetrics().
                    update(new EventBatchFailure(
                            LifecycleEvent.Type.EVENT_POST_FAILURE,
                            events, ex));
            invokeFailedCallback(events, ex);
        } catch (Exception e) {
            invokeFailedCallback(events, e);
        }
    } //end cancelled()    


    private void resend(Exception ex) {
        HttpSender sender = manager.getSender();
        try {
            LOG.error("channel  {} Failed to post event batch {}",
                    sender.getChannel(), events);
            sender.getChannelMetrics().
                    update(new EventBatchFailure(
                            LifecycleEvent.Type.EVENT_POST_FAILURE,
                            events, ex));
            events.addSendException(ex);
            LOG.warn("resending events through load balancer {}", events);
            sender.getConnection().getLoadBalancer().
                    sendRoundRobin(events, true); //will callback failed if max retries exceeded
        } catch (Exception e) {
            invokeFailedCallback(events, e);
        }
    }

    private void markBusyAndResend(String reply, int code) {
        HttpSender sender = manager.getSender();
        //tell rest of the system "server busy". Not exactly true but will cause channel to get marked unhealthy
        sender.getChannelMetrics().
                update(new Response(LifecycleEvent.Type.HEALTH_POLL_INDEXER_BUSY,
                        //FIXME -- it's not really a "HEALTH_POLL". Prolly change this Type to be named just "INDEXER_BUSY"
                        code, reply, sender.getBaseUrl()));

        resend(new HecServerBusyException(reply));
    }

    public void consumeEventPostOkResponse(String resp, int httpCode) throws Exception {
        //System.out.println("consuming event post response" + resp);

        HttpSender sender = manager.getSender();
        EventPostResponseValueObject epr = mapper.readValue(resp,
                EventPostResponseValueObject.class);
        if (epr.isAckIdReceived()) {
            events.setAckId(epr.getAckId()); //tell the batch what its HEC-generated ackId is.
        } else if (epr.isAckDisabled()) {
            throwConfigurationException(sender, httpCode, resp);
        }

        sender.getAcknowledgementTracker().handleEventPostResponse(epr, events);

        // start polling for acks
        manager.startPolling();

        sender.getChannelMetrics().update(new EventBatchResponse(
                LifecycleEvent.Type.EVENT_POST_OK, 200, resp,
                events, sender.getBaseUrl()));
    }

    private void throwConfigurationException(HttpSender sender, int httpCode, String resp) 
            throws HecConnectionStateException {
        sender.getChannelMetrics().
                update(new EventBatchResponse(
                        EVENT_POST_ACKS_DISABLED,
                        httpCode, resp, events, sender.getBaseUrl()));
        throw new HecConnectionStateException(
                "Event POST responded without ackId (acknowledgements are disabled on HEC endpoint).",
                CONFIGURATION_EXCEPTION);
    }

    @Override
    protected String getName() {
        return "Event Post";
    }

} //end HecHttpCallbacks
