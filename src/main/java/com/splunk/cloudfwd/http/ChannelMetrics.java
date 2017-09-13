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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 *
 * @author ghendrey
 */
public class ChannelMetrics extends LifecycleEventObservable implements LifecycleEventObserver {
  private static final Logger LOG = LoggerFactory.getLogger(ChannelMetrics.class.getName());

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
      case ACK_POLL_DISABLED:
      case HEALTH_POLL_INDEXER_BUSY:{ //INDEXER_BUSY is a normal operating condition, not a failure
        notifyObservers(e);
        return;
      }
    }
    if(e instanceof Response){
      Response r = (Response)e;
      if (r.getHttpCode() == 404) {
        LOG.debug("The indexer is in detention. Url: "
            + r.getUrl()
            + ", Code: " + r.getHttpCode()
            + ", Reply: ", r.getResp());
        notifyObservers(e);
      }
      else if(r.getHttpCode()!=200){
        LOG.error("Error from HEC endpoint in state "
            + e.getType().name()
            + ". Url: " + r.getUrl()
            + ", Code: " + r.getHttpCode()
            + ", Reply: " + r.getResp());
        EventBatch events = (e instanceof EventBatchResponse) ?
                ((EventBatchResponse)e).getEvents() : null;
        connection.getCallbacks().failed(events
            , getException(r.getHttpCode(), r.getResp(), r.getUrl()));
        notifyObservers(e); //might as well tell everyone there was a problem
      }
    }
  }

  private Exception getException(int httpCode, String reply, String url) {
    ObjectMapper mapper = new ObjectMapper();
    HecErrorResponseValueObject hecErrorResp;
    try {
      hecErrorResp = mapper.readValue(reply,
        HecErrorResponseValueObject.class);
    } catch (IOException ex) {
      throw new RuntimeException(ex.getMessage(), ex);
    }
    return new HecErrorResponseException(
      hecErrorResp.getText(), hecErrorResp.getCode(), url);
  }
}