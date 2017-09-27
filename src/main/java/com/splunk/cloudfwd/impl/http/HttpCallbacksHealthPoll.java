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

import com.splunk.cloudfwd.LifecycleEvent;
import static com.splunk.cloudfwd.LifecycleEvent.Type.HEALTH_POLL_FAILED;
import static com.splunk.cloudfwd.LifecycleEvent.Type.HEALTH_POLL_OK;
import java.io.IOException;
import org.slf4j.Logger;
import static com.splunk.cloudfwd.LifecycleEvent.Type.INDEXER_BUSY;

/**
 *
 * @author ghendrey
 */
class HttpCallbacksHealthPoll extends HttpCallbacksAbstract {
    private final Logger LOG;

    public HttpCallbacksHealthPoll(final HecIOManager m) {
        super(m);
        this.LOG = m.getSender().getConnection().getLogger(HttpCallbacksHealthPoll.class.getName());
    }

    private void handleHealthPollResponse(int statusCode, String reply) throws IOException {
        HttpSender sender = manager.getSender();
        LifecycleEvent.Type type = null;
        switch (statusCode) {
            case 200:
                type = HEALTH_POLL_OK;
                break;
            case 503:
                type = INDEXER_BUSY;
                warn(reply, statusCode);
                break;
            case 504:
                warn(reply, statusCode);
                break;
            default:
                type = error(reply, statusCode);
        }
        notify(type, statusCode, reply);

    }
    
    @Override
    public void failed(Exception ex) {
        try {
            LOG.error("Channel {} failed to poll health because {}",
                    getChannel(), ex);
            notifyFailed(HEALTH_POLL_FAILED, ex);
        } catch (Exception e) {
            error(ex);
        }
    }

    @Override
    public void cancelled() {
        try {
            Exception ex = new RuntimeException(
                    "HTTP post cancelled while polling for health on channel " + getChannel());
            notifyFailed(HEALTH_POLL_FAILED, ex);
        } catch (Exception ex) {
            error(ex);
        }
    }

    @Override
    public void completed(String reply, int code) {
        try {
            handleHealthPollResponse(code, reply);
        } catch (IOException ex) {
            error(ex);
        }
    }

    @Override
    protected String getName() {
        return "health poll";
    }
}
