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

import com.splunk.cloudfwd.LifecycleEvent;
import com.splunk.cloudfwd.impl.http.HecIOManager;
import com.splunk.cloudfwd.impl.http.lifecycle.Response;
import org.slf4j.Logger;

/**
 *
 * @author ghendrey
 */
public class GenericCoordinatedResponseHandler extends HttpCallbacksGeneric implements CoordinatedResponseHandler{

    private Logger LOG;
    private ResponseCoordinator coordinator;
    

    public GenericCoordinatedResponseHandler(HecIOManager m, LifecycleEvent.Type okType,
            LifecycleEvent.Type failType, String name) {
        super(m, okType, failType, name);  
        this.LOG = m.getSender().getConnection().getLogger(
                HttpCallbacksGeneric.class.
                getName());
    }
    
    public GenericCoordinatedResponseHandler(HecIOManager m, LifecycleEvent.Type okType,
            LifecycleEvent.Type failType, LifecycleEvent.Type gatewayTimeoutType,
            LifecycleEvent.Type indexerBusyType, String name) {
        super(m, okType, failType, gatewayTimeoutType, indexerBusyType, name);
        this.LOG = m.getSender().getConnection().getLogger(
                HttpCallbacksGeneric.class.
                        getName());
    }    

    /**
     *Overrides the base behavior which always notifies. We notify only when we have enough information from both 
     * coordinated response handlers to say definitively that the aggregated response is successful or not. Responses that
     * are not definitive, like an initial OK, or an OK following a failure/NOT OK must be ignored. This is why the methof
     * is called 'conditionallyUpdate'. Notify will get called by superclasses for all completed responses and failures, but 
     * not cancelled (which is why we override cancelled below).
     * @param e
     */
    @Override
    protected void notify(LifecycleEvent e) {
        coordinator.conditionallyUpate(e, getSender().getChannelMetrics());
    }
    
    @Override
    public void cancelled() {
        try {
            LOG.trace("cancelled '{}' on channel {}", getOperation(), getChannel());
            coordinator.cancel(null);
        } catch (Exception ex) {
            error(ex);
        }
    }  

    /**
     * @param coordinator the coordinator to set
     */
    @Override
    public void setCoordinator(
            ResponseCoordinator coordinator) {
        this.coordinator = coordinator;
    }

}
