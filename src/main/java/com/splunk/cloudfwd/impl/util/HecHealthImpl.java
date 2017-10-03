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
package com.splunk.cloudfwd.impl.util;

import com.splunk.cloudfwd.HecHealth;
import com.splunk.cloudfwd.LifecycleEvent;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.splunk.cloudfwd.error.HecServerErrorResponseException;
import org.slf4j.Logger;

/**
 * Describes the health
 *
 * @author ghendrey
 */
public class HecHealthImpl implements HecHealth {
    private CountDownLatch latch = new CountDownLatch(1); //wait for first setStatus to be called
    private Logger LOG;

    private boolean healthy;
    private LifecycleEvent status;
    private final HecChannel channel;

    public HecHealthImpl(HecChannel c, LifecycleEvent status) {
        this.channel = c;
        this.status = status;
        this.LOG = c.getConnection().getLogger(HecHealth.class.getName());
    }

    @Override
    public String toString() {
        return "HecHealthImpl{healthy=" + healthy + ", status=" + status + ", channel=" + channel + '}';
    }


    @Override
    public LifecycleEvent getStatus() {
        return this.status;
    }

    public void setStatus(LifecycleEvent status, boolean healthy) {
        this.status = status;
        this.healthy = healthy;
        this.latch.countDown();
    }

    @Override
    public String getUrl() {
        return channel.getSender().getBaseUrl();
    }

    /**
     * @return the healthy
     */
    @Override
    public boolean isHealthy() {
        return healthy;
    }

    /**
     * @return the channelId
     */
    public String getChannelId() {
        return channel.getChannelId();
    }

    @Override
    public RuntimeException getStatusException() {
       Exception e = getStatus().getException();
       if (null == e){
           return null;
       }
       if( ! (e instanceof RuntimeException)){
           return new RuntimeException(e.getMessage(), e);
       }
       return(RuntimeException)e;
    }


    @Override
    public boolean isMisconfigured() {
        Exception ex = getStatusException();
        if (ex instanceof HecServerErrorResponseException) {
            HecServerErrorResponseException error = (HecServerErrorResponseException)ex;
            // TODO: handle disabled tokens
            if (error.getLifecycleType() == LifecycleEvent.Type.ACK_DISABLED ||
                error.getLifecycleType() == LifecycleEvent.Type.INVALID_TOKEN ||
                error.getLifecycleType() == LifecycleEvent.Type.EVENT_POST_ACKS_DISABLED) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Exception getConfigurationException() {
        Exception e = null;
        if (isMisconfigured()) e = getStatusException();
        return e;
    }

    public boolean await(long wait, TimeUnit unit){
        try {
            return latch.await(wait, unit); //five minute timeout
        } catch (InterruptedException ex) {
           LOG.warn("Timed out waiting for HecHealth to become available.");
           return false;
        }        
    }
}
