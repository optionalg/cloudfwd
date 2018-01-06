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

import com.splunk.cloudfwd.error.HecIllegalStateException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;

/**
 * This class allows many HttpSender to share a single ClosableHttpAsyncClient. It counts references and closes the 
 * underlying CloseableHttpAsyncClient when the ref count reaches zero.
 *
 * @author ghendrey
 */
public class HttpClientWrapper {
    private static final int REQUESTORS_PER_CLIENT = 128;
    private LinkedList<CloseableHttpAsyncClientAndConnPoolControl>  clients = new LinkedList<>();
    private Map<HttpSender, CloseableHttpAsyncClientAndConnPoolControl> senderToClientMap = new HashMap<>();    
    
    HttpClientWrapper() {

    }

    public synchronized void releaseClient(HttpSender requestor) {
        CloseableHttpAsyncClientAndConnPoolControl c = senderToClientMap.remove(requestor);        
        if(c.removeReference(requestor)){ //true if there are no more references to this client
            this.clients.remove(c); 
        }   
    }

    public synchronized CloseableHttpAsyncClient getClient(
            HttpSender requestor, boolean disableCertificateValidation,
            String cert) {
        CloseableHttpAsyncClientAndConnPoolControl c;
        if (!isClientAllocatedFor(requestor)) {
            if(clients.isEmpty()){
                 c = newClient(disableCertificateValidation, cert, requestor);
            }else{
                 c = getMostRecentlyCreatedClient();
                if(c.getReferenceCount() == REQUESTORS_PER_CLIENT){
                    c = newClient(disableCertificateValidation, cert, requestor);
                }                 
            }
            c.addReference(requestor);        
        }else{ //already have a client mapped to this requestor
            c =getAllocatedClient(requestor);
        }

        return c.getClient();
    }    
    

    
    private CloseableHttpAsyncClientAndConnPoolControl getMostRecentlyCreatedClient(){
        return this.clients.getLast();
    }

    private boolean isClientAllocatedFor(HttpSender requestor) {
        return this.senderToClientMap.containsKey(requestor);
    }
    
    private CloseableHttpAsyncClientAndConnPoolControl getAllocatedClient(HttpSender requestor){
        CloseableHttpAsyncClientAndConnPoolControl c = this.senderToClientMap.get(requestor);
        if(null == c){
            throw new HecIllegalStateException("No client allocated for " + requestor.getBaseUrl(),
                    HecIllegalStateException.Type.NO_CLIENT_ALLOCATED);
        }
        return c;
    }

    private CloseableHttpAsyncClientAndConnPoolControl newClient(
            boolean disableCertificateValidation, String cert,
            HttpSender requestor) {
            try {
                CloseableHttpAsyncClientAndConnPoolControl c = new HttpClientFactory(disableCertificateValidation,
                        cert, requestor.getSslHostname(), requestor).build();
                c.getClient().start();    
                clients.add(c);
                senderToClientMap.put(requestor, c);
                return c;
            } catch (Exception ex) {
                throw new RuntimeException(ex.getMessage(), ex);
            }
    }

}
