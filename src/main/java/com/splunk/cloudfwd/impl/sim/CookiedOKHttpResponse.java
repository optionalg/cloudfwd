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
package com.splunk.cloudfwd.impl.sim;

import com.splunk.cloudfwd.impl.http.httpascync.HttpCallbacksEventPost;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.HeaderGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author anush
 */
public class CookiedOKHttpResponse extends CannedOKHttpResponse {
    protected static final Logger LOG = LoggerFactory.getLogger(CookiedOKHttpResponse.class.getName());
    String cookie;
    String syncAck = null;
    HeaderGroup headers = new HeaderGroup();
    
    public CookiedOKHttpResponse(HttpEntity entity, String cookie) {
        super(entity);
        this.cookie = cookie;
    }
    
    public CookiedOKHttpResponse(HttpEntity entity, String cookie, String syncAck) {
        this(entity, cookie);
        this.syncAck = syncAck;
        this.cookie = cookie;
    }
    
    @Override
    public Header getFirstHeader(String string) {
        for (Header h: headers.getAllHeaders()) {
            if(h.getName().equals(string)) { return h; } 
        }
        return null;
    }

    @Override
    public Header[] getHeaders(String headerName) {
        if (headerName.equalsIgnoreCase("Set-Cookie")) {
            headers.addHeader(new BasicHeader(headerName, this.cookie));
        }
        if (syncAck != null) {
            headers.addHeader(new BasicHeader(HttpCallbacksEventPost.SPLUNK_ACK_HEADER_NAME, this.syncAck));
        }
        LOG.info("getHeaders, headers=" + headers);
        return headers.getAllHeaders();
    }
}
