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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * This class allows us to keep track of one HttpClientWrapper per ssl hostname. This is needed because our simple
 * implementation of ssl hostname verification can accept only one ssl hostname. 
 * @author ghendrey
 */
public class HttpClientHostMapper {
        //key in the following Map is ssl hostname
        private final ConcurrentMap<String, HttpClientWrapper> clientMap = new ConcurrentHashMap<>(); 
        
        public HttpClientWrapper getClientWrapper(HttpSender sender){
            String sslHostname = sender.getSslHostname();
            return clientMap.computeIfAbsent(sslHostname, (key)->{
                return new HttpClientWrapper();
            });
        }
        
        public  int getNumHosts(){
            return clientMap.size();
        }
    
}
