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

import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.pool.ConnPoolControl;

/**
 * This class allows us to keep a handle to the  ConnPoolControl that is used by a CloseableHttpAsyncClient's 
 * PoolingNHttpClientConnectionManager. We need this handle so that we can dynamically adjust the number of 
 * Connections per Route on the fly as more HEC channels are added.
 * @author ghendrey
 */
public class CloseableHttpAsyncClientAndConnPoolControl {
    private CloseableHttpAsyncClient client;
    private ConnPoolControl connPoolControl;

    public CloseableHttpAsyncClientAndConnPoolControl(
            CloseableHttpAsyncClient client,
            ConnPoolControl control) {
        this.client = client;
        this.connPoolControl = control;
    }
    
    

    /**
     * @return the client
     */
    public CloseableHttpAsyncClient getClient() {
        return client;
    }

    /**
     * @return the ConnPoolControl
     */
    public ConnPoolControl getConPoolControl() {
        return connPoolControl;
    }
    
}
