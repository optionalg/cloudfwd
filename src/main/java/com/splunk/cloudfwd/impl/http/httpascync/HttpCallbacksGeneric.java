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
    private final LifecycleEvent.Type okType;
    private final LifecycleEvent.Type failType;

    public HttpCallbacksGeneric(HecIOManager m, LifecycleEvent.Type okType,
            LifecycleEvent.Type failType, String name) {
        super(m, name);
        this.okType = okType;
        this.failType = failType;
        this.LOG = m.getSender().getConnection().getLogger(HttpCallbacksGeneric.class.
                getName());
    }

    @Override
    public void completed(String reply, int httpCode) {
        LifecycleEvent.Type type = null;
        switch (httpCode) {
            case 200:
                type = okType;
                break;
            case 503:
            case 504:
            default: {
                try {
                    type = warn(reply, httpCode);
                } catch (IOException ex) {
                    error(ex);
                }
            }
        }
        notify(type, httpCode, reply);
    }

    @Override
    public void failed(Exception ex) {
        try {
            LOG.warn("Channel {} failed to'{}' because {}",
                    getChannel(), getName(), ex.getMessage());
            notifyFailed(failType, ex);
        } catch (Exception e) {
            error(ex);
        }
    }

}
