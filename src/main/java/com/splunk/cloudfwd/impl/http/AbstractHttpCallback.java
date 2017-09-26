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

import com.splunk.cloudfwd.EventBatch;
import com.splunk.cloudfwd.HecServerErrorResponseException;
import com.splunk.cloudfwd.LifecycleEvent;
import java.io.IOException;

import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;

/**
 *
 * @author ghendrey
 */
public abstract class AbstractHttpCallback implements FutureCallback<HttpResponse> {

  private final Logger LOG;
  protected final HecIOManager manager;

  AbstractHttpCallback(HecIOManager m) {
    LOG = m.getSender().getConnection().getLogger(AbstractHttpCallback.class.getName());
    this.manager = m;
  }

  @Override
  final public void completed(HttpResponse response) {
    int code = response.getStatusLine().getStatusCode();
    try {
      String reply = EntityUtils.toString(response.getEntity(), "utf-8");
      if(null == reply || reply.isEmpty()){
          LOG.error("reply with code {} was empty for function '{}'", getName());
      }
      completed(reply, code);      
    } catch (IOException e) {      
      LOG.error("Unable to get String from HTTP response entity", e);
    }      
  }

  public abstract void completed(String reply, int code);

    /**
     * Subclass should return the name indicative of it's purpose, such as "Health Poll" or "Event Post"
     * @return
     */
    protected abstract String getName();

    protected LifecycleEvent.Type invokeFailedWithHecServerResponseException(String reply,
            int statusCode, HttpSender sender) throws IOException {
        HecServerErrorResponseException e = NonBusyServerErrors.toErrorException(reply,
                statusCode, sender.getBaseUrl());
        invokeFailedCallback(e);
        return e.getType();
    }

    //Hardened to catch exceptions that could come from the application's failed callback
    protected void invokeFailedCallback(Exception ex) {
        try {
            LOG.error("Function '{}' Exception '{}'", getName(),  ex);
            manager.getSender().getConnection().getCallbacks().failed(null, ex);
        } catch (Exception e) {
            //if the application's callback is throwing an exception we have no way to handle this, other
            //than log an error
            LOG.error("Exception '{}'in ConnectionCallbacks.failed() for  '{}'",
                    ex, getName());
        }
    }
        

    //Hardened to catch exceptions that could come from the application's failed callback
    protected void invokeFailedCallback(EventBatch events, Exception ex) {
        try {
            LOG.error("Function '{}' Events  '{}' Exception '{}'", events, ex);
            manager.getSender().getConnection().getCallbacks().
                    failed(events, ex);
        } catch (Exception e) {
            //if the applicatoin's callback is throwing an exception we have no way to handle this, other
            //than log an error
            LOG.error("Exception '{}'in ConnectionCallbacks.failed() for  '{}' for events {}",
                    ex, events, getName());
        }           
    }

}
