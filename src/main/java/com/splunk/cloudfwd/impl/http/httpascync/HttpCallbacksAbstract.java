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

import com.splunk.cloudfwd.ConnectionCallbacks;
import com.splunk.cloudfwd.EventBatch;
import com.splunk.cloudfwd.error.HecServerErrorResponseException;
import com.splunk.cloudfwd.LifecycleEvent;
import com.splunk.cloudfwd.impl.ConnectionImpl;
import com.splunk.cloudfwd.impl.EventBatchImpl;
import com.splunk.cloudfwd.impl.http.HecIOManager;
import com.splunk.cloudfwd.impl.http.HttpSender;
import com.splunk.cloudfwd.impl.http.ServerErrors;
import com.splunk.cloudfwd.impl.http.lifecycle.EventBatchFailure;
import com.splunk.cloudfwd.impl.http.lifecycle.EventBatchResponse;
import com.splunk.cloudfwd.impl.http.lifecycle.RequestFailed;
import com.splunk.cloudfwd.impl.http.lifecycle.Response;
import com.splunk.cloudfwd.impl.util.HecChannel;
import java.io.IOException;
import java.util.Arrays;
import org.apache.http.Header;

import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;

/**
 *
 * @author ghendrey
 */
public abstract class HttpCallbacksAbstract implements FutureCallback<HttpResponse> {

  private final Logger LOG;
  protected final HecIOManager manager;

  HttpCallbacksAbstract(HecIOManager m) {
    LOG = m.getSender().getConnection().getLogger(HttpCallbacksAbstract.class.getName());
    this.manager = m;
  }

  @Override
  final public void completed(HttpResponse response) {
    try {    
        int code = response.getStatusLine().getStatusCode();
        Header[] headers = response.getHeaders("Set-Cookie");      
        LOG.debug("{} Cookies {}", getChannel(), Arrays.toString(headers));
        String reply = EntityUtils.toString(response.getEntity(), "utf-8");
        if(null == reply || reply.isEmpty()){
            LOG.error("reply with code {} was empty for function '{}'",code,  getName());
        }
        if(code != 200){
            LOG.error("NON-200 response code: {} server reply: {}", code, reply);
        }
        completed(reply, code);      
      } catch (IOException e) {      
        LOG.error("Unable to get String from HTTP response entity", e);
      }      
  }

  public abstract void completed(String reply, int code);
  
    protected void notify(final LifecycleEvent.Type type, int httpCode, String resp, EventBatchImpl events){
      notify(new EventBatchResponse(type, httpCode, resp, events, getBaseUrl()));
  }
  
  protected void notify(final LifecycleEvent.Type type, int httpCode, String resp){
      notify(new Response(type, httpCode, resp, getBaseUrl()));
  }
  
    protected void notifyFailed(final LifecycleEvent.Type type, EventBatchImpl events, Exception e){
      notify(new EventBatchFailure(
                            type,
                            events, e));
  }
  
  protected void notifyFailed(final LifecycleEvent.Type type, Exception e){
      notify(new RequestFailed(type,e));
  }
  
  protected void notify(LifecycleEvent e){
      manager.getSender().getChannelMetrics().update(e);
  }
  
  protected ConnectionImpl getConnection(){
      return manager.getSender().getConnection();
  }
  
  protected HecChannel getChannel(){
      return manager.getSender().getChannel();
  }
  
  protected String getBaseUrl(){
      return manager.getSender().getBaseUrl();
  }
  
  protected HttpSender getSender(){
      return manager.getSender();
  }
  
  protected ConnectionCallbacks getCallbacks(){
      return getConnection().getCallbacks();
  }

    /**
     * Subclass should return the name indicative of it's purpose, such as "Health Poll" or "Event Post"
     * @return
     */
    protected abstract String getName();

    protected LifecycleEvent.Type error(String reply,
            int statusCode) throws IOException {
        HecServerErrorResponseException e = ServerErrors.toErrorException(reply,
                statusCode, getBaseUrl());
        e.setContext(getName());
        error(e);
        return e.getType();
    }
    
    protected LifecycleEvent.Type warn(String reply,
            int statusCode) throws IOException {
        HecServerErrorResponseException e = ServerErrors.toErrorException(reply,
                statusCode, getBaseUrl());
        e.setContext(getName());
        warn(e);
        return e.getType();
    }    
    
    //Hardened to catch exceptions that could come from the application's failed callback
    protected void error(Exception ex) {
        try {
            LOG.error("System Error in Function '{}' Exception '{}'", getName(),  ex);
            getCallbacks().systemError(ex);
        } catch (Exception e) {
            //if the application's callback is throwing an exception we have no way to handle this, other
            //than log an error
            LOG.error("Exception '{}'in ConnectionCallbacks.systemError) for  '{}'",
                    ex, getName());
        }
    }
        

    //Hardened to catch exceptions that could come from the application's failed callback
    protected void invokeFailedEventsCallback(EventBatch events, Exception ex) {
        try {
            LOG.error("Failed events in Function '{}' Events  '{}' Exception '{}'",getName(), events, ex.getMessage());
            getCallbacks().
                    failed(events, ex);
        } catch (Exception e) {
            //if the applicatoin's callback is throwing an exception we have no way to handle this, other
            //than log an error
            LOG.error("Exception '{}'in ConnectionCallbacks.failed() for  '{}' for events {}",
                    ex, events, getName());
        }           
    }
    
    protected void warn(Exception ex) {
        try {
            LOG.warn("System Warning in Function '{}' Exception '{}'", getName(), ex.getMessage());
            getCallbacks().systemWarning(ex);
        } catch (Exception e) {
            //if the applicatoin's callback is throwing an exception we have no way to handle this, other
            //than log an error
            LOG.error("Exception '{}'in ConnectionCallbacks.systemWarning() for  '{}'",
                    ex, getName());
        }           
    }    
    

}
