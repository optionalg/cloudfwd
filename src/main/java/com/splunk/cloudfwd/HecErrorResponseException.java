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
 * <p>These are non-successful responses from an HEC endpoint.</p>
 *
 * The following error status codes and messages may
 * be returned from an HEC endpoint:
 * <p>
 *<table summary="ErrorCodes" border="1">
 * <tr>
 *  <th>Code</th>
 *  <th>Message</th>
 * </tr>
 *  <td> 1 </td> <td> Token disabled</td>
 * </tr>
 *<tr>
 * <td> 2 </td> <td> Token is required</td>
 *</tr>
 * <td> 3 </td> <td> Invalid authorization</td>
 *</tr>
 *<tr>
 * <td> 4 </td> <td> Invalid token</td>
 *</tr>
 * <td> 5 </td> <td> No data</td>
 *</tr>
 *<tr>
 * <td> 6 </td> <td> Invalid data format</td>
 *</tr>
 * <td> 7 </td> <td> Incorrect index</td>
 *</tr>
 *<tr>
 * <td> 8 </td> <td> Internal server error</td>
 *</tr>
 * <td> 9 </td> <td> Server is busy</td>
 *</tr>
 *<tr>
 * <td> 10 </td> <td> Data channel is missing</td>
 *</tr>
 * <td> 11 </td> <td> Invalid data channel</td>
 *</tr>
 *<tr>
 * <td> 12 </td> <td> Event field is required</td>
 *</tr>
 * <td> 13 </td> <td> Event field cannot be blank</td>
 *</tr>
 *<tr>
 * <td> 14 </td> <td> ACK is disabled</td>
 *</tr>
 *</table>
 *</p>
 * @author eprokop
 */
public class HecErrorResponseException extends Exception {
    private int code;
    private String url;

    public HecErrorResponseException(String message, int hecCode, String url) {
        super(message);
        this.code = hecCode;
        this.url = url;
    }

    public int getCode() {
        return code;
    }

    public String getUrl() {
        return url;
    }
}