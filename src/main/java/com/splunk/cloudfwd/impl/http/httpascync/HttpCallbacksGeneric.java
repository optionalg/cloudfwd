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
import java.io.IOException;
import org.slf4j.Logger;

/**
 *
 * @author ghendrey
 */
public class HttpCallbacksGeneric extends HttpCallbacksAbstract {

    private Logger LOG;
    //these are the lifecycels event types that will be raised when their condition occurs
    private final LifecycleEvent.Type okType;
    private final LifecycleEvent.Type failType;
    private LifecycleEvent.Type gatewayTimeoutType = null;
    private LifecycleEvent.Type indexerBusyType = null;

    public HttpCallbacksGeneric(HecIOManager m, LifecycleEvent.Type okType,
            LifecycleEvent.Type failType, String name) {
        super(m, name);
        this.okType = okType;
        this.failType = failType;
        this.LOG = m.getSender().getConnection().getLogger(
                HttpCallbacksGeneric.class.
                getName());
    }

    public HttpCallbacksGeneric(HecIOManager m, LifecycleEvent.Type okType,
            LifecycleEvent.Type failType, LifecycleEvent.Type gatewayTimeoutType,
            LifecycleEvent.Type indexerBusyType, String name) {
        this(m, okType, failType, name);
        this.gatewayTimeoutType = gatewayTimeoutType;
        this.indexerBusyType = indexerBusyType;
    }

    @Override
    public void completed(String reply, int httpCode) {
        try {
            LifecycleEvent.Type type = null;
            switch (httpCode) {
                case 200:
                    type = okType;
                    onOk(reply, httpCode);
                    break;
                case 503:
                    LOG.debug("503 response in HttpCallbacksGeneric {} on channel {}", getOperation(), getChannel());
                    if (null != indexerBusyType) {
                        type = indexerBusyType;
                    } else {
                        type = warn(reply, httpCode);
                    }
                    break;
                case 504:
                    if (null != gatewayTimeoutType) {
                        type = gatewayTimeoutType;
                    } else {
                        type = warn(reply, httpCode);
                    }
                    break;
                default:
                    type = warn(reply, httpCode);
            }
            notify(type, httpCode, reply);
        } catch (IOException ex) {            
            error(ex);
        }

    }

    @Override
    public void failed(Exception ex) {
        try {
            LOG.warn("Channel {} failed to'{}' because {}",
                    getChannel(), getOperation(), ex.getMessage());
            notifyFailed(failType, ex);
        } catch (Exception e) {
            error(ex);
        }
    }

    /**
     * Override this to provide hndling of Http200/OK. Default behavior does nothing (NoOp)
     * @param reply
     * @param httpCode
     * @throws java.io.IOException
     * @throws java.lang.Exception
     */
    protected void onOk(String reply, int httpCode) throws IOException {
        //noop by default
    }

    /**
     * @return the okType
     */
    public LifecycleEvent.Type getOkType() {
        return okType;
    }

    /**
     * @return the failType
     */
    public LifecycleEvent.Type getFailType() {
        return failType;
    }

    /**
     * @return the gatewayTimeoutType
     */
    public LifecycleEvent.Type getGatewayTimeoutType() {
        return gatewayTimeoutType;
    }

    /**
     * @return the indexerBusyType
     */
    public LifecycleEvent.Type getIndexerBusyType() {
        return indexerBusyType;
    }

}
