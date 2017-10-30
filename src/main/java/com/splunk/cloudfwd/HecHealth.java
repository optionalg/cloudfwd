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
package com.splunk.cloudfwd;

import com.splunk.cloudfwd.LifecycleEvent;
import com.splunk.cloudfwd.impl.util.HecChannel;
import java.time.Duration;

/**
 *
 * @author ghendrey
 */
public interface HecHealth {
    
    /**
     *  the duration since the channel was created, that this health belongs to
     * @return
     */
    public Duration getChannelAge();
    
    /**
     * the name of the thread that created the HecChannel that this health belongs to 
     * @return
     */
    public String getChannelCreatorThreadName();
    
    /**
     * returns the Duration that the channel has been decommissioned (or zero if not decommissioned)
     * @return
     */
    public Duration getTimeSinceDecomissioned();
    
    /**
     * When a channel is closed gracefully it begins to drain traffic. This is called quiescing. It doesn't release resources like HTTP client until
     * all the events have been acknowledged. This method returns the Duration that the channel has been in the quiesced state.
     * @return 0 Duration if channel was never close(), otherwise the Duration in close state before finishing release resources.
     */
    public Duration getQuiescedDuration();
    
    /**
     * If dead channel detection is enabled, and a channel has been detected "dead", this value tells how long the channel 
     * has been dead.
     * @return
     */
    public Duration getTimeSinceDeclaredDead();
    
    /**
     * IF the channel has finished closing (released all resources), then this will be a positive duration. Otherwise zero duration.
     * @return
     */
    public Duration getTimeSinceCloseFinished();
    
    
    /**
     * provides the time since the current value of isHealthy has been in its current state. For example, if isHealthy() is false, 
     * this method returns the Duration of time that the channel has been unhealthy, measured from the last moment the channel
     * that isHealthy() would have returned true.
     * @return
     */
    public Duration getTimeSinceHealthChanged();
    
    public LifecycleEvent getStatus();

    public String getUrl();

    public String getChannelId();
    
    public HecChannel getChannel();

    /**
     * @return true if healthu
     */
    boolean isHealthy();
    
        /**
     * @return true if channel is full
     */
    boolean isFull();
    

    /*
     * Return Exception responsible for LifecycleEvent returned by getStatus. This method has the same affect as
     * getStatus().getStatusException().
     * @return Exception that caused this state, or null if no Exception associated with this state.
     */
    public RuntimeException getStatusException();    

    public boolean isMisconfigured();

    public Exception getConfigurationException();
}
