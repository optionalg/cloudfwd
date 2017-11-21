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
import java.util.stream.Collectors;

import com.splunk.cloudfwd.impl.ConnectionImpl;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.slf4j.Logger;

/**
 * This class allows many HttpSender to share a single ClosableHttpAsyncClient. It counts references and closes the 
 * underlying CloseableHttpAsyncClient when the ref count reaches zero.
 *
 * @author ghendrey
 */
public class HttpClientWrapper {
    private CloseableHttpAsyncClient httpClient;
    private Set<HttpSender> requestors = new HashSet<>(); 
    private Logger LOG;

    HttpClientWrapper(ConnectionImpl c) {
        LOG = c.getLogger(HttpClientWrapper.class.getName());
        LOG.trace("constructor called");
    }

    public synchronized void releaseClient(HttpSender requestor) {
         
        LOG.debug("releaseClient: requestors: " +  
                requestors.stream().map(r -> r.getSslHostname()).collect(Collectors.toList()) + 
                ", requestor.getSslHostname(): " + requestor.getSslHostname());
         if (requestors.size() == 0) {
             throw new IllegalStateException("Illegal attempt to release http client, but http client is already closed.");
         }
        requestors.remove(requestor);
        if (requestors.size() == 0) {
            try {
                LOG.debug("releaseClient: attempting to close httpClient: " + httpClient);
                httpClient.close();
                LOG.debug("releaseClient: httpClient.isRunning?: " + httpClient.isRunning());
            } catch (IOException ex) {
                throw new RuntimeException(ex.getMessage(), ex);
            }
        }
    }

    public synchronized CloseableHttpAsyncClient getClient(
            HttpSender requestor, boolean disableCertificateValidation,
            String cert) {
        LOG.trace("getClient start: requestors: " +  
                requestors.stream().map(r -> r.getSslHostname()).collect(Collectors.toList()) +
                ", requestor.getSslHostname(): " + requestor.getSslHostname() +
                ", httpClient: " + httpClient);
        if (requestors.isEmpty()) {
            try {
                httpClient = new HttpClientFactory(disableCertificateValidation,
                        cert, requestor.getSslHostname(), requestor).build();
                httpClient.start();              
            } catch (Exception ex) {
                throw new RuntimeException(ex.getMessage(), ex);
            }
        }
        requestors.add(requestor);
        LOG.trace("getClient end: requestors: " +  
                requestors.stream().map(r -> r.getSslHostname()).collect(Collectors.toList()) +
                ", requestor.getSslHostname(): " + requestor.getSslHostname() +
                ", httpClient: " + httpClient);
        return httpClient;

    }

}
