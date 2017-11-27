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
import com.splunk.cloudfwd.impl.http.HttpBodyAndStatus;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
*
 * @author eprokop
 */


public class HecServerErrorResponseException extends RuntimeException {
    private static Set<Integer> nonRecoverableErrors = new HashSet<>(Arrays.asList(3, 10, 11));
    private static Set<Integer> recoverableConfigErrors = new HashSet<>(Arrays.asList(1, 2, 4, 7, 14, 400, 404)); //400 appears to be a splunk bug! Health poll returns http status 400 with code 400 for invalid token
    private static Set<Integer> recoverableDataErrors = new HashSet<>(Arrays.asList(5, 6, 12, 13));
    private static Set<Integer> recoverableServerErrors = new HashSet<>(Arrays.asList(8, 9));
    
    private HecErrorResponseValueObject serverRespObject;
    private HttpBodyAndStatus httpBodyAndStatus;
    private LifecycleEvent.Type lifecycleType;
    private String url;
    private Type errorType;
    private String context;

    /**
     * @return the httpBodyAndStatus
     */
    public HttpBodyAndStatus getHttpBodyAndStatus() {
        return httpBodyAndStatus;
    }




    public enum Type { NON_RECOVERABLE_ERROR, RECOVERABLE_CONFIG_ERROR, RECOVERABLE_DATA_ERROR, RECOVERABLE_SERVER_ERROR };


    public HecServerErrorResponseException(HecErrorResponseValueObject vo, HttpBodyAndStatus b, LifecycleEvent.Type type, String url) {
        this.serverRespObject = vo;
        this.httpBodyAndStatus = b;
        this.lifecycleType = type;        
        this.url = url;
        if(vo.getCode() != -1){
            //response was an HEC server response
            setErrorType(serverRespObject.getCode());
        }else{
            //response didn't even come from HEC!
            if(b.getStatusCode()==404 ){ //page not found
               this.errorType = Type.RECOVERABLE_CONFIG_ERROR; 
            }
        }
    }

    @Override
    public String toString() {
        return "HecServerErrorResponseException{" + "serverRespObject=" + serverRespObject + ", httpBodyAndStatus=" + httpBodyAndStatus + ", lifecycleType=" + lifecycleType + ", url=" + url + ", errorType=" + errorType + ", context=" + context + '}';
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