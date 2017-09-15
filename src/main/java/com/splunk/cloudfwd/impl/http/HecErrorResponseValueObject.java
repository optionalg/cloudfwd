package com.splunk.cloudfwd.impl.http;

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

/**
 *
 * @author eprokop
 */
public class HecErrorResponseValueObject {
    private String text;
    private int code;

    HecErrorResponseValueObject() {
    }

    /**
     * @return the code
     */
    public int getCode() {
        return code;
    }

    /**
     * @return the text
     */
    public String getText() {
        return text;
    }

}
