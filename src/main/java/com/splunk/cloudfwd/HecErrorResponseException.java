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

/**
 * These are non-successful responses from an HEC endpoint.
 *
 * The following error status codes and messages may
 * be returned from an HEC endpoint:
 *
 * Code:    Message:
 * 1        Token disabled
 * 2        Token is required
 * 3        Invalid authorization
 * 4        Invalid token
 * 5        No data
 * 6        Invalid data format
 * 7        Incorrect index
 * 8        Internal server error
 * 9        Server is busy
 * 10       Data channel is missing
 * 11       Invalid data channel
 * 12       Event field is required
 * 13       Event field cannot be blank
 * 14       ACK is disabled
 *
 * @author eprokop
 */
public class HecErrorResponseException extends Exception {
    private int code;
    private String url;
    private String message;

    public HecErrorResponseException() {}

    public HecErrorResponseException(String message, int hecCode, String url) {
        super(message);
        this.code = hecCode;
        this.url = url;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public int getCode() {
        return code;
    }

    public String getUrl() {
        return url;
    }

    public String getMessage() { return message;}
}