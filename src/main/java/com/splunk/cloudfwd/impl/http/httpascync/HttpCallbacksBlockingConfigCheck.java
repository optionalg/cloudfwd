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

import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.LifecycleEvent;
import static com.splunk.cloudfwd.PropertyKeys.PREFLIGHT_RETRIES;
import com.splunk.cloudfwd.error.HecMaxRetriesException;
import com.splunk.cloudfwd.error.HecServerErrorResponseException;
import com.splunk.cloudfwd.impl.ConnectionImpl;
import com.splunk.cloudfwd.impl.http.HecIOManager;
import com.splunk.cloudfwd.impl.http.ServerErrors;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.http.ConnectionClosedException;
import org.slf4j.Logger;

/**
 *
 * @author ghendrey
 */
public class HttpCallbacksBlockingConfigCheck extends HttpCallbacksAbstract {

    private final Logger LOG;
    private final CountDownLatch latch = new CountDownLatch(1);
    private HecServerErrorResponseException configProblems;
    private Exception exception;
    private int numTries;
    private boolean started;


    public HttpCallbacksBlockingConfigCheck(HecIOManager m, ConnectionImpl c) {
        super(m, c);
        this.LOG = getConnection().getLogger(
                HttpCallbacksBlockingConfigCheck.class.
                getName());
    }

    public void setStarted(boolean b) {
        started = b;
    }

    public static class TimeoutException extends Exception {

        public TimeoutException(String message) {
            super(message);
        }

    }
    
    /**
     *The config check is not associated with a channel, so we just return the name of this config checker. This is OK
     * because getChannel is only used for logging the context of the activity.
     * @return
     */
    @Override
    protected Object getChannel(){
      return getName(); 
    }
    
    @Override
    protected String getName() {
        return "Config Check";
    }

    
    public HecServerErrorResponseException getConfigProblems(long timeoutMs)
            throws Exception {
        if(!started){
            throw new IllegalStateException("Attempt to getConfigProblems before started");
        }
        if (!latch.await(timeoutMs, TimeUnit.MILLISECONDS)) {
            throw new TimeoutException("Timed out waiting for server response");
        }
        if(null != exception){ //an Excption was caught while waiting on latch
            throw exception;
        }
        return configProblems;
    }

    private void handleResponse(int statusCode, String reply) throws IOException {
        LifecycleEvent.Type type;
        switch (statusCode) {
            case 503:
            case 504:
                retry();                
                break;
            case 200:
                LOG.info("HEC config check is good on {}", getBaseUrl());
                latch.countDown(); //getConfigProblems() can return now
                break;
            default: //various non-200 errors such as 400/ack-is-disabled
                this.configProblems = ServerErrors.toErrorException(reply, statusCode, super.getBaseUrl());
                latch.countDown();
        }

    }

    private void retry() {
        if(++numTries <=getSettings().getMaxPreflightRetries()){
            LOG.warn("retrying config checks checks on {}", getBaseUrl());
            manager.configCheck(this); //retry
        }else{
            String msg = "Config Checks failed  " + PREFLIGHT_RETRIES+"="
                    + getSettings().getMaxPreflightRetries() + " exceeded";
            exception = new HecMaxRetriesException(msg);
            latch.countDown();
        }
    }

    @Override
    public void completed(String reply, int code) {
        try {
            handleResponse(code, reply);
        } catch (Exception ex) {
            LOG.error(
                    "failed to unmarshal server response in pre-flight health check {}",
                    reply);
            exception = ex;
            latch.countDown();
        }
    }

    @Override
    public void failed(Exception ex) {
        if(ex instanceof ConnectionClosedException){
            LOG.debug("Caught ConnectionClosedException."
                    + " This is expected when a channel is closed while a config check is in process.");
            return;
        }
        LOG.error(
                "HEC confg check via /ack endpoint failed with exception {} on {}",ex.getMessage(), getBaseUrl(),
                ex);
        exception = ex;
        latch.countDown();
    }

    @Override
    public void cancelled() {
        LOG.warn("HEC config check cancelled");
        exception = new Exception(
                "HEC config check cancelled");
        latch.countDown();
    }

}
