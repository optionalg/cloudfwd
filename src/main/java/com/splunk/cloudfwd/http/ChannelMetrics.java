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

import com.splunk.cloudfwd.http.lifecycle.LifecycleEventObserver;
import com.splunk.cloudfwd.http.lifecycle.LifecycleEvent;
import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.http.lifecycle.LifecycleEventObservable;
import com.splunk.cloudfwd.http.lifecycle.Response;
import java.util.logging.Logger;

/**
 *
 * @author ghendrey
 */
public class ChannelMetrics extends LifecycleEventObservable implements LifecycleEventObserver {

  private static final Logger LOG = Logger.getLogger(ChannelMetrics.class.
          getName());
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
  private boolean lastHealthCheck;
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
      case EVENT_POST_OK: {
        //System.out.println("NOTIFYING EVENT_POST_OK");
        notifyObservers(e);
        return;
      }
      case ACK_POLL_OK: {
        notifyObservers(e);
        return;
      }

      case HEALTH_POLL_OK: {
        lastHealthCheck = true;
        notifyObservers(e);
        return;
      }
      case HEALTH_POLL_NOT_OK: {
        lastHealthCheck = false;
        notifyObservers(e);
        return;
      }
      case HEALTH_POLL_FAILED: {
        lastHealthCheck = false;
        notifyObservers(e);
        return;
      }
    }
    if(e instanceof Response){
      if(((Response) e).getHttpCode()!=200){
      Response r = (Response)e;  
      String msg = "Server did not return OK/200. Code: " + r.getHttpCode() + ", reply:" + ((Response) e).getResp();
      connection.getCallbacks().failed(null, new RuntimeException(msg));
      }
    }
  }
}