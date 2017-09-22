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
package com.splunk.cloudfwd;

import java.util.*;

/**
 * <p>These are non-successful responses from an HEC endpoint.</p>
 *
 * The following error status codes and messages may
 * be returned from an HEC endpoint:
 *
 *<table summary="ErrorCodes" border="1">
 * <tr>
 *  <th>Code</th>
 *  <th>Message</th>
 * </tr>
 *  <td> 1 </td> <td> Token disabled. This is a recoverable configuration error.</td>
 * </tr>
 *<tr>
 * <td> 2 </td> <td> Token is required. This is a recoverable configuration error.</td>
 *</tr>
 * <td> 3 </td> <td> Invalid authorization. This is a non-recoverable error.</td>
 *</tr>
 *<tr>
 * <td> 4 </td> <td> Invalid token. This is a recoverable configuration error.</td>
 *</tr>
 * <td> 5 </td> <td> No data. This is a recoverable data error.</td>
 *</tr>
 *<tr>
 * <td> 6 </td> <td> Invalid data format. This is a recoverable data error.</td>
 *</tr>
 * <td> 7 </td> <td> Incorrect index. This is a recoverable configuration error.</td>
 *</tr>
 *<tr>
 * <td> 8 </td> <td> Internal server error. This is a recoverable server error.</td>
 *</tr>
 * <td> 9 </td> <td> Server is busy. This is a recoverable server error.</td>
 *</tr>
 *<tr>
 * <td> 10 </td> <td> Data channel is missing. This is a non-recoverable error.</td>
 *</tr>
 * <td> 11 </td> <td> Invalid data channel. This is a non-recoverable error.</td>
 *</tr>
 *<tr>
 * <td> 12 </td> <td> Event field is required. This is a recoverable data error.</td>
 *</tr>
 * <td> 13 </td> <td> Event field cannot be blank. This is a recoverable data error.</td>
 *</tr>
 * @author eprokop
 */


public class HecServerErrorResponseException extends Exception {
    private int code;
    private String url;
    private Type errorType;

    private Set<Integer> nonRecoverableErrors = new HashSet<>(Arrays.asList(3, 10, 11));
    private Set<Integer> recoverableConfigErrors = new HashSet<>(Arrays.asList(1, 2, 4, 7, 14));
    private Set<Integer> recoverableDataErrors = new HashSet<>(Arrays.asList(5, 6, 12, 13));
    private Set<Integer> recoverableServerErrors = new HashSet<>(Arrays.asList(8, 9));

    public enum Type { NON_RECOVERABLE_ERROR, RECOVERABLE_CONFIG_ERROR, RECOVERABLE_DATA_ERROR, RECOVERABLE_SERVER_ERROR };

    public HecServerErrorResponseException(String message) {
        super(message);
    }

    public HecServerErrorResponseException(String message, int hecCode, String url) {
        super(message);
        this.code = hecCode;
        this.url = url;
        setErrorType(hecCode);
    }
    
    

    public void setCode(int code) {
        this.code = code;
    }

    public void setUrl(String url) {
        this.url = url;
    }


    public int getCode() {
        return code;
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