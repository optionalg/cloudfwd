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
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;

/**
 * This class allows many HttpSender to share a single ClosableHttpAsyncClient. It counts references and closes the 
 * underlying CloseableHttpAsyncClient when the ref count reaches zero.
 *
 * @author ghendrey
 */
public class HttpClientWrapper {

    private CloseableHttpAsyncClientAndConnPoolControl  httpClientAndConnPoolControl;
    private Set<HttpSender> requestors = new HashSet<>();    

    HttpClientWrapper() {

    }

    public synchronized void releaseClient(HttpSender requestor) {
           
         if (requestors.size() == 0) {
             throw new IllegalStateException("Illegal attempt to release http client, but http client is already closed.");
         }
        requestors.remove(requestor);        
        if (requestors.size() == 0) {
            try {
                httpClientAndConnPoolControl.getClient().close();
            } catch (IOException ex) {
                throw new RuntimeException(ex.getMessage(), ex);
            }
        }else{
            adjustConnPoolSize();
        }
    }

    public synchronized CloseableHttpAsyncClient getClient(
            HttpSender requestor, boolean disableCertificateValidation,
            String cert) {
        if (requestors.isEmpty()) {
            try {
                httpClientAndConnPoolControl = new HttpClientFactory(disableCertificateValidation,
                        cert, requestor.getSslHostname(), requestor).build();
                httpClientAndConnPoolControl.getClient().start();              
            } catch (Exception ex) {
                throw new RuntimeException(ex.getMessage(), ex);
            }
        }
        //the first time we add a requestor to the set, add will return true and we can update the connection pool
        //to reflect the new number of HttpSenders that exist. We want the pool to have as many connecitons as there
        //are HttpSender instances
        if(requestors.add(requestor)){
//            adjustConnPoolSize();
        }
        return httpClientAndConnPoolControl.getClient();
    }    
    
    private void adjustConnPoolSize(){                
        httpClientAndConnPoolControl.getConPoolControl().setDefaultMaxPerRoute(Math.max(requestors.size()*10,HttpClientFactory.INITIAL_MAX_CONN_PER_ROUTE));
        //We expect only one Route per HttpSender, but nevertheless, for safety we double the number of requestors in computing the max total connections. This is
        //a decent idea becuase for each HttpSender there will be multiple pollers (health, acks) in addition to event posting
        httpClientAndConnPoolControl.getConPoolControl().setMaxTotal(Math.max(requestors.size()*8,HttpClientFactory.INITIAL_MAX_CONN_TOTAL));
    }
}
