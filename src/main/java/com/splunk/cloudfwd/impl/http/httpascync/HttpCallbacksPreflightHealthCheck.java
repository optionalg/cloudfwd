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
import static com.splunk.cloudfwd.LifecycleEvent.Type.PREFLIGHT_BUSY;
import java.io.IOException;
import org.slf4j.Logger;
import static com.splunk.cloudfwd.LifecycleEvent.Type.PREFLIGHT_OK;
import org.apache.http.ConnectionClosedException;

/**
  Code    HTTP status	HTTP status code	Status message
    0	200	OK                                     Success                
    1	403	Forbidden                          Token disabled
    2	401	Unauthorized                     Token is required
    3	401	Unauthorized                     Invalid authorization
    4	403	Forbidden                          Invalid token
    5	400	Bad Request                       No data
    6	400	Bad Request                       Invalid data format
    7	400	Bad Request                       Incorrect index
    8	500	Internal Error	  Internal server error
    9	503	Service Unavailable	  Server is busy
    10	400	Bad Request                       Data channel is missing
    11	400	Bad Request                       Invalid data channel
    12	400	Bad Request                       Event field is required
    13	400	Bad Request	                     Event field cannot be blank
    14	400	Bad Request	                     ACK is disabled 
 * @author ghendrey
 */
public class HttpCallbacksPreflightHealthCheck extends HttpCallbacksAbstract {
    private final Logger LOG;

    public HttpCallbacksPreflightHealthCheck(HecIOManager m) {
        super(m);
        this.LOG =getConnection().getLogger(HttpCallbacksPreflightHealthCheck.class.
                getName());
    }

    private void handleResponse(int statusCode, String reply) throws IOException {
        LifecycleEvent.Type type;
        switch (statusCode) {            
            case 503:  
                warn(reply, statusCode);
                type = PREFLIGHT_BUSY;
                break;
            case 504:  
                warn(reply, statusCode);
                type = LifecycleEvent.Type.PREFLIGHT_GATEWAY_TIMEOUT;
                break;
            case 200:
                LOG.info("HEC preflight check is good on {}", getChannel());
                type = PREFLIGHT_OK;
                break;
            default: //various non-200 errors such as 400/ack-is-disabled
                type = error(reply, statusCode);
        }
        notify(type, statusCode, reply);
    }
    
    
    @Override
    public void completed(String reply, int code) {
        try {
            handleResponse(code, reply);
        } catch (Exception ex) {
            LOG.error(
                    "failed to unmarshal server response in pre-flight health check {}",
                    reply);
            error(ex);
        } finally {
            manager.startHealthPolling();
        }
    }
    


    @Override
    public void failed(Exception ex) {
        if(ex instanceof ConnectionClosedException){
            LOG.debug("Caught ConnectionClosedException."
                    + " This is expected when a channel is closed while a pre-flight check is in process.");
            return;
        }
        LOG.error(
                "HEC pre-flight health check via /ack endpoint failed with exception {} on {}",ex.getMessage(), getChannel(),
                ex);
        error(ex);
        // TODO: retry if this fails.. otherwise channel just sits around until it is reaped
    }

    @Override
    public void cancelled() {
        LOG.warn("HEC pre-flight health check cancelled");
        error(new Exception(
                "HEC pre-flight health check via /ack endpoint cancelled."));
    }


    @Override
    protected String getName() {
        return "Preflight checks";
    }

}
