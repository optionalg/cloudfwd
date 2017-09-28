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

/**
 * Describes the health
 *
 * @author ghendrey
 */
public class HecHealthImpl implements HecHealth {

    private String url;
    private final String channelId;
    private boolean healthy;
    private LifecycleEvent status;

    public HecHealthImpl(String channelId, String url, LifecycleEvent status) {
        this.channelId = channelId;
        this.url = url;
        this.status = status;
    }

    @Override
    public String toString() {
        return "HecHealthImpl{" + "url=" + url + ", channelId=" + channelId + ", healthy=" + healthy + ", status=" + status + '}';
    }



    @Override
    public LifecycleEvent getStatus() {
        return this.status;
    }

    public void setStatus(LifecycleEvent status, boolean healthy) {
        this.status = status;
        this.healthy = healthy;
    }

    @Override
    public String getUrl() {
        return this.url;
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
        return channelId;
    }

    @Override
    public Exception getException() {
       return getStatus().getException();
    }
}
