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

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;

/**
 * This class allows many HttpSender to share a single ClosableHttpAsyncClient.
 *
 * @author ghendrey
 */
public class HttpClientWrapper {
    private static CloseableHttpAsyncClient httpClient;
    private static Set<HttpSender> requestors = new HashSet<>();

    private HttpClientWrapper() {

    }

    public static synchronized void releaseClient(HttpSender requestor) {
           
         if (requestors.size() == 0) {
             throw new IllegalStateException("Illegal attempt to release http client, but http client is already closed.");
         }
        requestors.remove(requestor);
        if (requestors.size() == 0) {
            try {
                httpClient.close();
            } catch (IOException ex) {
                throw new RuntimeException(ex.getMessage(), ex);
            }
        }
    }

    public static synchronized CloseableHttpAsyncClient getClient(
            HttpSender requestor, boolean disableCertificateValidation,
            String cert, String host) {
        if (requestors.isEmpty()) {
            try {
                httpClient = new HttpClientFactory(disableCertificateValidation,
                        cert, host, requestor).build();
                httpClient.start();              
            } catch (Exception ex) {
                throw new RuntimeException(ex.getMessage(), ex);
            }
        }
        requestors.add(requestor);  
        return httpClient;

    }

}
