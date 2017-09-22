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
import com.splunk.cloudfwd.impl.EventBatchImpl;
import com.splunk.cloudfwd.HecServerErrorResponseException;
import com.splunk.cloudfwd.impl.ConnectionImpl;
import com.splunk.cloudfwd.impl.http.lifecycle.EventBatchResponse;
import com.splunk.cloudfwd.impl.http.lifecycle.LifecycleEvent;
import com.splunk.cloudfwd.impl.http.lifecycle.LifecycleEventObservable;
import com.splunk.cloudfwd.impl.http.lifecycle.LifecycleEventObserver;
import com.splunk.cloudfwd.impl.http.lifecycle.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 *
 * @author ghendrey
 */
public class ChannelMetrics extends LifecycleEventObservable implements LifecycleEventObserver {

    private final Logger LOG;

    /*
  private long eventPostCount;
  private long eventPostOKCount;
  private long eventPostNotOKCount;
  private long eventPostFailureCount;
  private long ackPollCount;
  private long ackPollOKCount;
  private long ackPollNotOKCount;
  private long ackPollFailureCount;

     */
    // health-related
    //private boolean lastHealthCheck;
    // private long healthPollOKCount;
    // private long healthPollNotOKCount;
    // private long healthPollFailureCount;
    public ChannelMetrics(ConnectionImpl c) {
        super(c);
        this.LOG = c.getLogger(ChannelMetrics.class.getName());
    }

    @Override
    public void update(LifecycleEvent e) {
        switch (e.getType()) {
            case EVENT_POST_OK:
            case ACK_POLL_OK:
            case HEALTH_POLL_OK:
            case N2K_HEC_HEALTHY:
            {
              notifyObservers(e);
              return;
            }
            case ACK_POLL_DISABLED:
            //INDEXER_BUSY is a normal operating condition, not a failure
            case HEALTH_POLL_INDEXER_BUSY:
            case SPLUNK_IN_DETENTION:
              // invalid token state, only used for health check api
              // otherwise should throw exception and call failed() callback
            case N2K_INVALID_TOKEN:
            case N2K_INVALID_AUTH:
            {
                if (e instanceof Response) {
                  Response r = (Response) e;
                  LOG.debug("Splunk instance not optimal. Url: "
                    + r.getUrl()
                    + ", Code: " + r.getHttpCode()
                    + ", Reply: ", r.getResp());
                }
                notifyObservers(e);
                return;
            }
            default:
              break;
        }
        if (e instanceof Response) {
            Response r = (Response) e;
            if (r.getHttpCode() != 200) {
                LOG.error("Error from HEC endpoint in state "
                        + e.getType().name()
                        + ". Url: " + r.getUrl()
                        + ", Code: " + r.getHttpCode()
                        + ", Reply: " + r.getResp());
                EventBatchImpl events = (e instanceof EventBatchResponse)
                        ? ((EventBatchResponse) e).getEvents() : null;
                connection.getCallbacks().failed(events, getException(r.
                        getHttpCode(), r.getResp(), r.getUrl()));
                notifyObservers(e); //might as well tell everyone there was a problem
            }
        }
    }

    private Exception getException(int httpCode, String reply, String url) {
        ObjectMapper mapper = new ObjectMapper();
        HecErrorResponseValueObject hecErrorResp;
        try {
            LOG.trace("unmarshalling HecErrorResponseValueObject from " + reply);
            hecErrorResp = mapper.readValue(reply,
                    HecErrorResponseValueObject.class);
            LOG.trace("HecErrorResponseValueObject is {}", hecErrorResp);
        } catch (IOException ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
        return new HecServerErrorResponseException(
                hecErrorResp.getText(), hecErrorResp.getCode(), url);
    }
}
