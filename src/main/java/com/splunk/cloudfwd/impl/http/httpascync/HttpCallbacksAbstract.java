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
import com.splunk.cloudfwd.ConnectionSettings;
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
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
  private final HecIOManager manager;
  
  //in the case of health polling, we hit both the ack and the health endpoint
 private AtomicBoolean alreadyNotOk; //tells us if one of the two responses has already come back NOT OK
 private boolean expectingTwoResponses; //true only when we are doing health polling which hits ack and health endpoint
 private AtomicInteger responseCount;
 //Due to ELB session cookies, preflight checks, which are the first requests sent on a channel, must be serialized.
 //This latch enables the serialization of requests.
 private LifecycleEventLatch latch; //provided by external source who wants to serialize two *requests*
  
  HttpCallbacksAbstract(HecIOManager m) {
    LOG = m.getSender().getConnection().getLogger(HttpCallbacksAbstract.class.getName());
    this.manager = m;
  }
  
    /**
     * Causes notify() on OK to await the latch. Used to syn
     * @param respCount
     * @see 
     */
    public void setTwoResponseStateTracker(AtomicBoolean alreadyNotOk, AtomicInteger respCount){
        this.alreadyNotOk = alreadyNotOk;
        this.expectingTwoResponses = true;
        this.responseCount = respCount;
    }
    public void setLatch(LifecycleEventLatch latch) {
        this.latch = latch;
    }

  @Override
  final public void completed(HttpResponse response) {
    try {    
        int code = response.getStatusLine().getStatusCode();
        Header[] headers = response.getHeaders("Set-Cookie");      
        LOG.debug("{} Cookies {}", getChannel(), Arrays.toString(headers));
        String reply = EntityUtils.toString(response.getEntity(), "utf-8");
        if(null == reply || reply.isEmpty()){
            LOG.warn("reply with code {} was empty for function '{}'",code,  getName());
        }
        if(code != 200){
            LOG.warn("NON-200 response code: {} server reply: {}", code, reply);
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
  
  //all flavors of notifyXXX will eventually call down to this method
  protected void notify(LifecycleEvent e){
      if(expectingTwoResponses ){ //health polling hits ack and health endpoints
              synchronized(alreadyNotOk){
                if(null != latch){
                    latch.countDown(e);//got a response or failed. Preflight can now look at e and decide how to proceed
                }
                responseCount.incrementAndGet();
                if(alreadyNotOk.get()){
                    if(e.isOK()){
                        LOG.debug("Ignoring OK response from '{}' because already got not ok from other endpoint on '{}'.", 
                                getName(), getChannel());
                        return; //must not update status to OK, when the other of two expected response isn't NOT OK.
                    }
                }
                if(responseCount.get()==1 && e.isOK()){
                        LOG.debug("Ignoring FIRST OK response from '{}' because waiting for other resp on '{}'.", 
                                getName(), getChannel());                    
                    return; //discard the first response if it is OK. We have to wait for the 2nd, which will be authoratitive
                }
                alreadyNotOk.set(!e.isOK()); //record fact that we saw a not OK
                //the update must occur in the synchronized block when we are latched on two requests   
                manager.getSender().getChannelMetrics().update(e);
                return;
              }             
      }else{ //normal condition where we don't depend on any other response
        manager.getSender().getChannelMetrics().update(e);
      }
  }
  
  protected ConnectionImpl getConnection(){
    return manager.getSender().getConnection();
  }
  
  protected Object getChannel(){
//      if(null != connection){ //when we explicitely construct with a Connection, it is because the sender does not have channel
//          throw new IllegalStateException("Channel is not available from sender.");
//      }
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
        return e.getLifecycleType();
    }
    
    protected LifecycleEvent.Type warn(String reply,
            int statusCode) throws IOException {
        HecServerErrorResponseException e = ServerErrors.toErrorException(reply,
                statusCode, getBaseUrl());
        e.setContext(getName());
        warn(e);
        return e.getLifecycleType();
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
    
        protected LifecycleEvent.Type invokeFailedEventsCallback(EventBatch events, String reply,
            int statusCode) throws IOException {
        HecServerErrorResponseException e = ServerErrors.toErrorException(reply,
                statusCode, getBaseUrl());
        e.setContext(getName());
            invokeFailedEventsCallback(events, e);
        return e.getLifecycleType();
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
            LOG.warn("{} System Warning in Function '{}' Exception '{}'", getChannel(), getName(), ex.getMessage());
            getCallbacks().systemWarning(ex);
        } catch (Exception e) {
            //if the applicatoin's callback is throwing an exception we have no way to handle this, other
            //than log an error
            LOG.error("{} Exception '{}'in ConnectionCallbacks.systemWarning() for  '{}'",
                    getChannel(), ex.getMessage(), getName());
        }           
    }    
    
    protected ConnectionSettings getSettings(){
        return getConnection().getSettings();
    }

    /**
     * @return the manager
     */
    public HecIOManager getManager() {
        return manager;
    }
    

}
