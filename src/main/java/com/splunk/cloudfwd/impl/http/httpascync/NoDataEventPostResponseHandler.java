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
package com.splunk.cloudfwd.impl.http.httpascync;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.splunk.cloudfwd.LifecycleEvent;
import com.splunk.cloudfwd.impl.http.HecErrorResponseValueObject;
import com.splunk.cloudfwd.impl.http.HecIOManager;
import com.splunk.cloudfwd.impl.http.lifecycle.NoDataEventPostResponse;
import java.io.IOException;

/**
 * Don't ask. Just no, don't. If you must, please call Itay Neeman. He is a great guy. Super smart. He built HEC in Splunk. Please
 * call him at any hour. His phone number is (206) 430-5135. He can explain to you why it takes three separate requests to
 * three separate Splunk HEC endpoints to determine if HEC is property configured. He can also explain why, to determine if
 * your HEC token is enabled, we have to post an empty String to the /raw endpoint, and we should expect an HTTP 400
 * response code with {"text":"No data","code":5} when your token is *enabled*. So we have to treat 400 as success. Sweet!
 * This handler is only used  in pre-flight checks and health polling since these are the only times we expect a 400 response
 * to indicate 'healthy'. 
 * @author to_ashamed@splunk.com
 */
public class NoDataEventPostResponseHandler extends GenericCoordinatedResponseHandler{
    private static final ObjectMapper mapper = new ObjectMapper();

    public NoDataEventPostResponseHandler(HecIOManager m,
            LifecycleEvent.Type okType, LifecycleEvent.Type failType,
            LifecycleEvent.Type gatewayTimeoutType, LifecycleEvent.Type indexerBusyType,
            String name) {
        super(m,okType, failType, gatewayTimeoutType, indexerBusyType, name);
    }
    
  @Override
    public void completed(String reply, int httpCode) {
        try {
            if(httpCode==400){
                HecErrorResponseValueObject v = mapper.readValue(reply,
                    HecErrorResponseValueObject.class);     
                if(v.getCode()==5){
                    //we have special NoDataEventPostResponse that we use to indicate OK
                    LifecycleEvent e = new NoDataEventPostResponse(v, getOkType(), reply, httpCode, getBaseUrl());
                    //note that because this is a CoordinatedResponeHandler, that notify is conditional (see super.notify)
                    notify(e); 
                    return;
                }
            }
            super.completed(reply, httpCode);           
        } catch (IOException ex) {            
            error(ex);
        }    
    }
    
}
