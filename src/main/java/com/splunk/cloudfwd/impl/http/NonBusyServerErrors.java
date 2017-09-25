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
import com.splunk.cloudfwd.impl.http.lifecycle.LifecycleEvent;
import static com.splunk.cloudfwd.impl.http.lifecycle.LifecycleEvent.Type.ACK_POLL_DISABLED;
import static com.splunk.cloudfwd.impl.http.lifecycle.LifecycleEvent.Type.INVALID_TOKEN;
import static com.splunk.cloudfwd.impl.http.lifecycle.LifecycleEvent.Type.N2K_INVALID_AUTH;
import static com.splunk.cloudfwd.impl.http.lifecycle.LifecycleEvent.Type.SPLUNK_IN_DETENTION;
import static com.splunk.cloudfwd.impl.http.lifecycle.LifecycleEvent.Type.UNHANDLED_NON_200;
import java.io.IOException;

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
public class NonBusyServerErrors {

    private static final ObjectMapper mapper = new ObjectMapper();

    public static LifecycleEvent.Type type(int statusCode, String reply) throws IOException {
        HecErrorResponseValueObject r = mapper.readValue(reply,
                HecErrorResponseValueObject.class);
        LifecycleEvent.Type type = UNHANDLED_NON_200;
        switch (statusCode) {
            case 400:
                // determine code in reply, must be 14 for disabled
                if (14 == r.getCode()) {
                    type = ACK_POLL_DISABLED;
                }
                break;
            case 404:
                //undocumented?
                type = SPLUNK_IN_DETENTION;
                break;
            case 403:
                //HTTPSTATUS_FORBIDDEN
                type = INVALID_TOKEN;
                break;
            case 401:
                //HTTPSTATUS_UNAUTHORIZED
                type = N2K_INVALID_AUTH;
                break;
        }
        return type;
    }

}
