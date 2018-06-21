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
import com.splunk.cloudfwd.error.HecNonStickySessionException;
import com.splunk.cloudfwd.impl.http.HecIOManager;
import com.splunk.cloudfwd.impl.http.ChannelCookies;
import com.splunk.cloudfwd.impl.http.lifecycle.PreflightFailed;
import com.splunk.cloudfwd.impl.util.HecChannel;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;

import java.io.IOException;

/**
 *
 * @author cloudbuilder
 */
public class CoordinatedFirstResponseHandler extends GenericCoordinatedResponseHandler implements CoordinatedResponseHandler{

    private Logger LOG;
    private ResponseCoordinator coordinator;
    

    public CoordinatedFirstResponseHandler(HecIOManager m, LifecycleEvent.Type okType,
                                           LifecycleEvent.Type failType, String name) {
        super(m, okType, failType, name);  
        this.LOG = m.getSender().getConnection().getLogger(
                CoordinatedFirstResponseHandler.class.
                getName());
    }
    
    public CoordinatedFirstResponseHandler(HecIOManager m, LifecycleEvent.Type okType,
                                           LifecycleEvent.Type failType, LifecycleEvent.Type gatewayTimeoutType,
                                           LifecycleEvent.Type indexerBusyType, String name) {
        super(m, okType, failType, gatewayTimeoutType, indexerBusyType, name);
        this.LOG = m.getSender().getConnection().getLogger(
                HttpCallbacksGeneric.class.
                        getName());
    }
    
    @Override
    public void completed(HttpResponse response) {
        try {
            ChannelCookies cookies = new ChannelCookies((HecChannel)getChannel(), response);
            getSender().setCookies(cookies);
            int code = response.getStatusLine().getStatusCode();
            //handleCookies(response);
            String reply = EntityUtils.toString(response.getEntity(), "utf-8");
            if(null == reply || reply.isEmpty()){
                LOG.warn("reply with code {} was empty for function '{}'",code,  getOperation());
            }
            completed(reply, code, response);
        } catch (IOException e) {
            LOG.error("Unable to get String from HTTP response entity", e);
            failed(new RuntimeException("Unable to get String from HTTP response entity, response=" + response));
        } catch (HecNonStickySessionException e) {
            LOG.warn("Got HecNonStickySessionException exception, failing the channel", e);
            failed(e);
            LOG.debug("finished failing the channel", e);
        } catch (Exception e) {
            LOG.error("Unexpected exception handling complete callback for response=" + response +
                    " exception=" + e + "exception_message=" + e.getMessage());
            failed(new RuntimeException("Unexpected exception handling complete callback for response=" + response +
                    " exception=" + e + "exception_message=" + e.getMessage()));
        }
    }
}
