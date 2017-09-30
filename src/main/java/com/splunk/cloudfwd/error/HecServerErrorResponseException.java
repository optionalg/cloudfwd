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
package com.splunk.cloudfwd.error;

import com.splunk.cloudfwd.LifecycleEvent;
import com.splunk.cloudfwd.impl.http.HecErrorResponseValueObject;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
*
 * @author eprokop
 */


public class HecServerErrorResponseException extends Exception {
    private static Set<Integer> nonRecoverableErrors = new HashSet<>(Arrays.asList(3, 10, 11));
    private static Set<Integer> recoverableConfigErrors = new HashSet<>(Arrays.asList(1, 2, 4, 7, 14));
    private static Set<Integer> recoverableDataErrors = new HashSet<>(Arrays.asList(5, 6, 12, 13));
    private static Set<Integer> recoverableServerErrors = new HashSet<>(Arrays.asList(8, 9));
    
    private HecErrorResponseValueObject serverRespObject;
    private String httpResponseBody;
    private LifecycleEvent.Type lifecycleType;
    private String url;
    private Type errorType;
    private String context;




    public enum Type { NON_RECOVERABLE_ERROR, RECOVERABLE_CONFIG_ERROR, RECOVERABLE_DATA_ERROR, RECOVERABLE_SERVER_ERROR };


    public HecServerErrorResponseException(HecErrorResponseValueObject vo, String serverReply, LifecycleEvent.Type type, String url) {
        this.serverRespObject = vo;
        this.httpResponseBody = serverReply;
        this.lifecycleType = type;        
        this.url = url;
        setErrorType(serverRespObject.getCode());
    }

    @Override
    public String toString() {
        return "HecServerErrorResponseException{" + "serverRespObject=" + serverRespObject + ", serverReply=" + httpResponseBody + ", lifecycleType=" + lifecycleType + ", url=" + url + ", errorType=" + errorType + ", context=" + context + '}';
    }
    

    
    
    /**
     * @return the httpResponseBody
     */
    public String getHttpResponseBody() {
        return httpResponseBody;
    }

    /**
     * @return the lifecycleType
     */
    public LifecycleEvent.Type getLifecycleType() {
        return lifecycleType;
    }

    /**
     * @return the message
     */
    public String getMessage() {
        return toString();
    }

    /**
     * @return the context
     */
    public String getContext() {
        return context;
    }

    /**
     * @param context the context to set
     */
    public void setContext(String context) {
        this.context = context;
    }
    

    public void setUrl(String url) {
        this.url = url;
    }


    public int getCode() {
        return serverRespObject.getCode();
    }
    
    public String getHecErrorText(){
        return serverRespObject.getText();
    }
    
    public int getInvalidEventNumber(){
        return serverRespObject.getInvalidEventNumber();
    }

    public String getUrl() {
        return url;
    }

    public Type getErrorType() { return errorType; };

    private void setErrorType(Integer hecCode) {
        if (nonRecoverableErrors.contains(hecCode)) {
            errorType = Type.NON_RECOVERABLE_ERROR;
        } else if (recoverableConfigErrors.contains(hecCode)) {
            errorType =  Type.RECOVERABLE_CONFIG_ERROR;
        } else if (recoverableDataErrors.contains(hecCode)) {
            errorType =  Type.RECOVERABLE_DATA_ERROR;
        } else if (recoverableServerErrors.contains(hecCode)) {
            errorType =  Type.RECOVERABLE_SERVER_ERROR;
        }
    }
}