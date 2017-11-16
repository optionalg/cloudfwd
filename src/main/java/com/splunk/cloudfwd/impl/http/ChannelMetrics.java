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
import com.splunk.cloudfwd.impl.util.HecChannel;
import com.splunk.cloudfwd.metrics.ChannelEventMetric;
import org.slf4j.Logger;

/**
 *
 * @author ghendrey
 */
public class ChannelMetrics extends LifecycleEventObservable implements LifecycleEventObserver {

    private final Logger LOG;
    private final HecChannel channel;

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
    public ChannelMetrics(ConnectionImpl c, HecChannel channel) {
        super(c);
        this.channel = channel;
        this.LOG = c.getLogger(ChannelMetrics.class.getName());
    }
    
    @Override
    public void update(LifecycleEvent e) {
        switch(e.getType()) {
            // whitelist the types we want to track for metrics
            case EVENT_POST_OK:
            case ACK_POLL_OK:
            case EVENT_POST_INDEXER_BUSY:
            case INDEXER_BUSY:
            case EVENT_POST_FAILED:
            case EVENT_POST_NOT_OK:
            case EVENT_POST_GATEWAY_TIMEOUT:
            case GATEWAY_TIMEOUT:
            case HEC_HTTP_400_ERROR:
            case PREFLIGHT_OK:
            case PREFLIGHT_FAILED:
                connection.emitMetric(
                    new ChannelEventMetric(connection, channel, e));
                break;
            default:
                break;
        }
        notifyObservers(e);
    }
}
