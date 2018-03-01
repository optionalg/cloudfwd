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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.splunk.cloudfwd.error.HecServerErrorResponseException;
import com.splunk.cloudfwd.LifecycleEvent;
import static com.splunk.cloudfwd.LifecycleEvent.Type.INVALID_TOKEN;
import static com.splunk.cloudfwd.LifecycleEvent.Type.UNHANDLED_NON_200;
import java.io.IOException;
import static com.splunk.cloudfwd.LifecycleEvent.Type.ACK_DISABLED;
import static com.splunk.cloudfwd.LifecycleEvent.Type.DATA_CHANNEL_MISSING_OR_INVALID;
import static com.splunk.cloudfwd.LifecycleEvent.Type.INVALID_AUTH;
import static com.splunk.cloudfwd.LifecycleEvent.Type.GATEWAY_TIMEOUT;
import static com.splunk.cloudfwd.LifecycleEvent.Type.INDEXER_BUSY;
import static com.splunk.cloudfwd.LifecycleEvent.Type.INDEXER_IN_DETENTION;
import static com.splunk.cloudfwd.LifecycleEvent.Type.HEC_HTTP_400_ERROR;

/**
  Code    HTTP status	HTTP status code	Status message
    0	200	OK                                     Success                
    1	403	Forbidden                          Token disabled
    2	401	Unauthorized                     Token is required
    3	401	Unauthorized                     Invalid authorization
    4	403	Forbidden                          Invalid token
    5	400	Bad Request                       No data
    6	400	Bad Request                       Invalid data format
    7	400	Bad Request                       Incorrect index
    8	500	Internal Error	  Internal server error
    9	503	Service Unavailable	  Server is busy
    10	400	Bad Request                       Data channel is missing
    11	400	Bad Request                       Invalid data channel
    12	400	Bad Request                       Event field is required
    13	400	Bad Request	                     Event field cannot be blank
    14	400	Bad Request	                     ACK is disabled 
 * @author ghendrey
 */
public class ServerErrors {

    private static final ObjectMapper mapper = new ObjectMapper();
    
    public static HecServerErrorResponseException toErrorException(String reply,
            int statusCode, String url) throws IOException {

        final LifecycleEvent.Type type =  hecType(statusCode, reply);       
        final HttpBodyAndStatus b = new HttpBodyAndStatus(statusCode, reply);
        if(reply != null && !reply.isEmpty()){
            try{
                HecErrorResponseValueObject r = mapper.readValue(reply,
                    HecErrorResponseValueObject.class);               
                return new HecServerErrorResponseException(r, b, type,  url);
            }catch(Exception e){ //response like 404/"not found" will fail to unmarshal into HecErrorResponseValueObject (not hjson)
                return new HecServerErrorResponseException(new HecErrorResponseValueObject(), b, type, url);
            }
        }else{
            //server response without text such as 504 gate way timeout
            return new HecServerErrorResponseException(new HecErrorResponseValueObject(), b, type, url);
        }
    }

    private static LifecycleEvent.Type hecType(int statusCode, String reply) throws IOException {
        LifecycleEvent.Type type = null;
        HecErrorResponseValueObject r = null;
        switch (statusCode) {
            case 200:
                throw new IllegalStateException("200 is not an error code.");
            case 400:
                r = mapper.readValue(reply,
                            HecErrorResponseValueObject.class);  
                switch(r.getCode()){
                    case 5://no data
                    case 6://invalid data format 
                    case 7://incorrect index
                    case 12://event field is required
                    case 13://event field cannot be blank
                        type = LifecycleEvent.Type.EVENT_POST_NOT_OK; //the codes above all apply to event post
                         break;
                    case 14://ack is disabled...can apply to ack poll or event post
                        type = ACK_DISABLED;
                        break;
                    case 400://this appears to be a splunk bug! We expect http 403 with code=4 text="invalid token"
                        type = INVALID_TOKEN;
                        break;
                    case 10://data channel is missing
                    case 11://invalid data channel    
                        type = DATA_CHANNEL_MISSING_OR_INVALID;
                    default: 
                        type= HEC_HTTP_400_ERROR;                                            
                }//end code switch
                break;
            case 404:
                //undocumented?
                type = INDEXER_IN_DETENTION;
                break;
            case 403:
                //HTTPSTATUS_FORBIDDEN
                type = INVALID_TOKEN;
                break;
            case 401:
                type = INVALID_AUTH;
                break;
            case 503:
                type = INDEXER_BUSY;
                break;       
            case 504:
                type = GATEWAY_TIMEOUT;
                break;     
            default:
                type = UNHANDLED_NON_200;                
        }
        return type;
    }
    
}
