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
 * Represents an illegal state in Cloudfwd that requires resolution by the client.
 *
 * @author ghendrey
 */
public class HecConnectionStateException extends IllegalStateException {

    public enum Type {
        ALREADY_SENT,
        ALREADY_ACKNOWLEDGED,
        WRONG_EVENT_FORMAT_FOR_ENDPOINT,
        ALREADY_FLUSHED,
        CONNECTION_CALLBACK_NOT_SET,
        SEND_ON_CLOSED_CONNECTION,
        CONFIGURATION_EXCEPTION
    }
    private final Type type;

    /**
     * @return the type
     */
    public Type getType() {
        return type;
    }

    public HecConnectionStateException(String s, Type t) {
        super(s);
        this.type = t;
    }

    public String toString() {
        return super.toString() + type;
    }
}