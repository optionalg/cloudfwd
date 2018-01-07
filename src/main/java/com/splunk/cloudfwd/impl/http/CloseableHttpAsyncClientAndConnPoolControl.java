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
    private Set<HttpSender> referenceHolders = new HashSet<>();

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

    int getReferenceCount() {
        return referenceHolders.size();
    }

    void addReference(HttpSender referenceHolder) {
        if(referenceHolders.add(referenceHolder)){
            adjustConnPoolSize();
        }
    }
    
    private void adjustConnPoolSize(){                
//        this.connPoolControl.setDefaultMaxPerRoute(0);//Math.max(referenceHolders.size(),HttpClientFactory.INITIAL_MAX_CONN_PER_ROUTE));
//        //We expect only one Route per HttpSender, but nevertheless, for safety we double the number of requestors in computing the max total connections. This is
//        //a decent idea becuase for each HttpSender there will be multiple pollers (health, acks) in addition to event posting
//        this.connPoolControl.setMaxTotal(0);//Math.max(referenceHolders.size(),HttpClientFactory.INITIAL_MAX_CONN_TOTAL));
//       this.connPoolControl.setDefaultMaxPerRoute(0);//Math.max(referenceHolders.size(),HttpClientFactory.INITIAL_MAX_CONN_PER_ROUTE));

    }    

    //returns true if there are no reference holders left after this removeReference
    boolean removeReference(HttpSender requestor) {
         if (referenceHolders.size() == 0) {
             throw new IllegalStateException("Illegal attempt to release http client, but http client is already closed.");
         }
        referenceHolders.remove(requestor);        
        if (referenceHolders.isEmpty()) {
            try {
                System.out.println("CLOSING CLIENT");
                getClient().close();
                return true;
            } catch (IOException ex) {
                throw new RuntimeException(ex.getMessage(), ex);
            }
        }else{
            adjustConnPoolSize();
        }
        return false;
    }
    
}
