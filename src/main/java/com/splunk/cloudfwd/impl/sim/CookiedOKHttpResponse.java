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

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.message.BasicHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author anush
 */
public class CookiedOKHttpResponse extends CannedOKHttpResponse {
    protected static final Logger LOG = LoggerFactory.getLogger(CookiedOKHttpResponse.class.getName());
    String cookie;

    public CookiedOKHttpResponse(HttpEntity entity, String cookie) {
        super(entity);
        this.cookie = cookie;
    }

    public String getCookie() {
        return cookie;
    }

    @Override
    public Header[] getHeaders(String headerName) {
        if (headerName.equalsIgnoreCase("Set-Cookie")) {
            Header[] h = new Header[1];
            h[0] = new BasicHeader(headerName, this.cookie);
            return h;
        }
        else {
            LOG.debug("getHeaders returns no headers other than for \"Set-Cookie\" -- this is a MOCK.");
            //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            return new Header[]{};
        }
    }
}