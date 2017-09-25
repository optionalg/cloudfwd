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
import com.splunk.cloudfwd.impl.http.lifecycle.RequestFailed;
import com.splunk.cloudfwd.impl.http.lifecycle.Response;
import java.io.IOException;
import org.slf4j.Logger;

/**
 *
 * @author ghendrey
 */
class AckPollHttpCallbacks extends AbstractHttpCallback {

    private static final ObjectMapper mapper = new ObjectMapper();
    private final HecIOManager manager;
    private final Logger LOG;

    public AckPollHttpCallbacks(final HecIOManager m) {
        super(m.getSender().getConnection());
        this.manager = m;
        this.LOG = m.getSender().getConnection().getLogger(
                AckPollHttpCallbacks.class.getName());
    }

    @Override
    public void completed(String reply, int code) {
        try {
            HttpSender sender = manager.getSender();
            LOG.trace("channel {} reply {} ", sender.getChannel(), reply);
            if (code == 200) {
                consumeAckPollResponse(reply);
            } else {
                ackPollResponseNotOK(reply, sender, code);
            }            
        } catch (Exception e) {
            invokeFailedCallback(e);
        }finally{
            manager.setAckPollInProgress(false);
        }
    }

    @Override
    public void failed(Exception ex) {
        try {
            HttpSender sender = manager.getSender();
            LOG.error("Channel {} failed to poll acks because {}",
                    sender.getChannel(), ex);
            //Note that we dot invoke any failed callbacks. We just treat an ack poll failure as an indicator of unhealthy channel
            sender.getChannelMetrics().
                    update(new RequestFailed(
                            LifecycleEvent.Type.ACK_POLL_FAILURE,
                            ex));                        
        } catch (Exception e) {
            invokeFailedCallback(e);
        }finally{
            manager.setAckPollInProgress(false);
        }
    }

    @Override
    public void cancelled() {
        try {
            HttpSender sender = manager.getSender();
            LOG.error("Ack poll  cancelled on channel  {}",
                    sender.getChannel());           
        } catch (Exception e) {
            invokeFailedCallback(e);
        }finally{
            manager.setAckPollInProgress(false);
        }
    }

    private void consumeAckPollResponse(String resp) {
        try {
            AckPollResponseValueObject ackPollResp = mapper.
                    readValue(resp, AckPollResponseValueObject.class);
            manager.getAcknowledgementTracker().handleAckPollResponse(
                    ackPollResp);
        } catch (IOException ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }
    
    private void ackPollResponseNotOK(String reply, HttpSender sender, int code) {
        LOG.error("non-200 response from ack poll {}", reply);
        Exception e = new HecServerErrorResponseException(reply,
                code, manager.getSender().getBaseUrl());
        invokeFailedCallback(e);        
        sender.getChannelMetrics().
                update(new Response(LifecycleEvent.Type.ACK_POLL_NOT_OK,
                        code, reply, sender.getBaseUrl()));
    }    

    //Hardened to catch exceptions that could come from the application's failed callback
    private void invokeFailedCallback(Exception ex) {
        try {
            LOG.error(
                    "ack polling failed with exception {}",
                    ex);
            manager.getSender().getConnection().getCallbacks().
                    failed(null, ex);
        } catch (Exception e) {
            //if the application's callback is throwing an exception we have no way to handle this, other
            //than log an error
            LOG.error(
                    "Caught exception in ConnectionCallbacks.failed for ack polling, {}",
                    ex);
        }
    }

}
