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
package com.splunk.cloudfwd.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.splunk.cloudfwd.EventBatch;
import com.splunk.cloudfwd.HecErrorResponseException;
import com.splunk.cloudfwd.http.lifecycle.*;
import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.sim.HecErrorResponse;

import java.io.IOException;

/**
 *
 * @author ghendrey
 */
public class ChannelMetrics extends LifecycleEventObservable implements LifecycleEventObserver {

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

  public ChannelMetrics(Connection c) {
    super(c);
  }

  @Override
  public void update(LifecycleEvent e) {
    handleLifecycleEvent(e);
  }

  private void handleLifecycleEvent(LifecycleEvent e) {
    switch (e.getType()) {
      case EVENT_POST_OK: 
      case ACK_POLL_OK: 
      case HEALTH_POLL_OK:
      case PREFLIGHT_CHECK_OK:
      case HEALTH_POLL_INDEXER_BUSY:{ //INDEXER_BUSY is a normal operating condition, not a failure
        notifyObservers(e);
        return;
      }
    }
    if(e instanceof Response){
      Response r = (Response)e;
      if(r.getHttpCode()!=200){

//        String msg = "Server did not return OK/200 in state "+e.getType().name()+". HTTP code: " + r.getHttpCode() + ", reply:" + ((Response) e).getResp();

        EventBatch events = null;
        if (e instanceof EventBatchResponse) {
          events = ((EventBatchResponse)e).getEvents();
        }
        connection.getCallbacks().failed(events, getException(r.getResp(),));
        notifyObservers(e); //might as well tell everyone there was a problem
      }
    }
  }

  private HecErrorResponseException getException(String reply) {
    ObjectMapper mapper = new ObjectMapper();
    HecErrorResponseValueObject hecErrorResp;
    try {
      hecErrorResp = mapper.readValue(reply,
              HecErrorResponseValueObject.class);
    } catch (IOException ex) {
      throw new RuntimeException(ex.getMessage(), ex);
    }
//    LOG.error("Error from HEC endpoint. Channel: " + sender.getChannel().
//            toString() + " Code: " + httpCode + " Reply: " + reply);
    return new HecErrorResponseException(
            hecErrorResp.getText(), hecErrorResp.getCode(), sender.
            getBaseUrl());
  }
}