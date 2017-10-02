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

import com.splunk.cloudfwd.error.HecServerErrorResponseException;
import com.splunk.cloudfwd.impl.http.HttpSender;
import com.splunk.cloudfwd.impl.util.HecChannel;

/**
 *
 * @author ghendrey
 */
public class ConfigStatus {
    
    private final HecChannel channel;
    private final RuntimeException exception;   

    public ConfigStatus(HecChannel channel, RuntimeException problem) {
        this.channel = channel;
        this.exception = problem;
    }

    @Override
    public String toString() {
        return "ConfigStatus{" + "channel=" + channel + ", problem=" + exception + '}';
    }
    
    /**
     *
     * @return true if getException returns null
     */
    public boolean isOk(){
        return exception==null;
    }



    /**
     * @return the exception, or null if there was no exception
     */
    public RuntimeException getProblem() {
        return exception;
    }

    /**
     * @return the channel
     */
    public HecChannel getChannel() {
        return channel;
    }
    
}
