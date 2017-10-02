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

import static com.splunk.cloudfwd.PropertyKeys.PREFLIGHT_RETRIES;
import com.splunk.cloudfwd.error.HecMaxRetriesException;
import com.splunk.cloudfwd.impl.http.HecIOManager;
import com.splunk.cloudfwd.impl.http.ServerErrors;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import org.slf4j.Logger;

/**
 *
 * @author ghendrey
 */
public class HttpCallbacksBlockingConfigCheck extends HttpCallbacksAbstract {

    private final Logger LOG;
    private final CountDownLatch latch = new CountDownLatch(1);
    //private HecServerErrorResponseException errorResponse;
    private RuntimeException exception;
    private int numTries;
    private boolean started;


    public HttpCallbacksBlockingConfigCheck(HecIOManager m) {
        super(m);
        this.LOG = getConnection().getLogger(
                HttpCallbacksBlockingConfigCheck.class.
                getName());
    }

    public void setStarted(boolean b) {
        started = b;
    }
    
    
    @Override
    protected String getName() {
        return "Config Check";
    }

    
    public RuntimeException getException() {
        if(!started){
            throw new IllegalStateException("Attempt to getConfigProblems before started");
        }
        try {
            latch.await() ;
        } catch (InterruptedException ex) {
            LOG.error("Interrupted waiting for config status on channel {}", getChannel());
        }
        return exception;
    }

    private void handleResponse(int statusCode, String reply) throws IOException, InterruptedException {
        switch (statusCode) {
            case 503:
                LOG.warn("Server busy while attempting config check on {}, waiting 60 seconds to retry", getChannel());
                Thread.sleep(60000);
            case 504:
                retry();                
                break;
            case 200:
                LOG.info("HEC config check is good on {}", getChannel());
                latch.countDown(); //getConfigProblems() can return now
                break;
            default: //various non-200 errors such as 400/ack-is-disabled                
                this.exception = ServerErrors.toErrorException(reply, statusCode, super.getBaseUrl());
                LOG.warn("HEC config check not OK '{}' '{}'", getChannel(), exception.getMessage());
                latch.countDown();
        }
    }

    private void retry() {
        if(++numTries <=getSettings().getMaxPreflightRetries()){
            LOG.warn("retrying config checks checks on {}", getChannel());
            manager.configCheck(this); //retry
        }else{
            String msg = "Config Checks failed  " + PREFLIGHT_RETRIES+"="
                    + getSettings().getMaxPreflightRetries() + " exceeded on " + getChannel();
            LOG.warn(msg);
            exception = new HecMaxRetriesException(msg);
            latch.countDown();
        }
    }

    @Override
    public void completed(String reply, int code) {
        try {
            handleResponse(code, reply);
        }catch (Exception ex) {
            LOG.error(
                    "Caught exception  '{}' trying to handle response  '{}'",ex.getMessage(),
                    reply);
            exception = new RuntimeException(ex.getMessage(), ex);
            latch.countDown();
        }
    }

    @Override
    public void failed(Exception ex) {
        /*
        if(ex instanceof ConnectionClosedException){
            LOG.debug("Caught ConnectionClosedException."
                    + " This is expected when a channel is closed while a config check is in process.");
            return;
        }*/
        LOG.warn(
                "HEC config check  failed with exception  '{}' on '{}'",ex.getMessage(), getChannel());
        retry(); //failures such as
        //exception = ex;
        //latch.countDown();
    }

    @Override
    public void cancelled() {
        LOG.warn("HEC config check cancelled");
        exception = new RuntimeException(
                "HEC config check cancelled");
        latch.countDown();
    }

}
