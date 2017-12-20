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
import com.splunk.cloudfwd.LifecycleEvent;
import com.splunk.cloudfwd.impl.http.AckPollResponseValueObject;
import com.splunk.cloudfwd.impl.http.HecIOManager;
import static com.splunk.cloudfwd.LifecycleEvent.Type.ACK_POLL_NOT_OK;
import java.io.IOException;
import org.slf4j.Logger;

/**
 *
 * @author ghendrey
 */
public class HttpCallbacksAckPoll extends HttpCallbacksAbstract {

    private static final ObjectMapper mapper = new ObjectMapper();
    private final Logger LOG;
    public static final String Name = "ack_poll";

    public HttpCallbacksAckPoll(final HecIOManager m) {
        super(m, Name);
        this.LOG = getConnection().getLogger(HttpCallbacksAckPoll.class.getName());
    }

    @Override
    public void completed(String reply, int code) {
        try {
            switch(code){
                case 200:
                    consumeAckPollResponse(reply);
                    break;
                case 503: //busy
                    LOG.debug("503 response from ack poll on channel={}", getChannel());
                    warn(reply, code);                    
                    break;
                case 504: //elb gatewy timeout
                    warn(reply, code);
                    break;
                default:
                    //error(reply, code);
                    warn(reply, code);
                    notify(ACK_POLL_NOT_OK, code, reply);
            }         
        } catch (Exception e) {
            error(e);
        }finally{
            getManager().setAckPollInProgress(false);
        }
    }

    @Override
    public void failed(Exception ex) {
        try {
            LOG.warn("Channel {} failed to poll acks because {}",
                    getChannel(), ex);    
            notifyFailed(LifecycleEvent.Type.ACK_POLL_FAILURE, ex);
        } catch (Exception e) {
            error(e);
        }finally{
            getManager().setAckPollInProgress(false);
        }
    }


    private void consumeAckPollResponse(String resp) throws IOException {
        AckPollResponseValueObject ackPollResp = mapper.
                readValue(resp, AckPollResponseValueObject.class);
        getManager().getAcknowledgementTracker().handleAckPollResponse(
                ackPollResp);
    }       

}
