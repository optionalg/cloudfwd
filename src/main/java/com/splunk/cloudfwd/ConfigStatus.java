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
import com.splunk.cloudfwd.impl.ConnectionImpl;
import com.splunk.cloudfwd.impl.http.HttpSender;

/**
 *
 * @author ghendrey
 */
public class ConfigStatus {
    
    private final String url;
    private final HecServerErrorResponseException problem;
    private final ConnectionImpl outer;

    public ConfigStatus(HttpSender sender, HecServerErrorResponseException problem,
            final ConnectionImpl outer) {
        this.outer = outer;
        this.url = sender.getBaseUrl();
        this.problem = problem;
    }

    @Override
    public String toString() {
        return "ConfigStatus{" + "url=" + url + ", problem=" + problem + '}';
    }

    /**
     * @return the url
     */
    public String getUrl() {
        return url;
    }

    /**
     * @return the problem
     */
    public HecServerErrorResponseException getProblem() {
        return problem;
    }
    
}
