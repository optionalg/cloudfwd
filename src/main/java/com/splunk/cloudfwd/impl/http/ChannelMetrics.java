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

import com.splunk.cloudfwd.impl.ConnectionImpl;
import com.splunk.cloudfwd.LifecycleEvent;
import com.splunk.cloudfwd.impl.http.lifecycle.LifecycleEventObservable;
import com.splunk.cloudfwd.impl.http.lifecycle.LifecycleEventObserver;
import org.slf4j.Logger;

/**
 *
 * @author ghendrey
 */
public class ChannelMetrics extends LifecycleEventObservable implements LifecycleEventObserver {

    private final Logger LOG;

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
    public ChannelMetrics(ConnectionImpl c) {
        super(c);
        this.LOG = c.getLogger(ChannelMetrics.class.getName());
    }
    
    @Override
    public void update(LifecycleEvent e) {
        notifyObservers(e);
    }
}
