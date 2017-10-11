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
package com.splunk.cloudfwd.impl.http.lifecycle;

import com.splunk.cloudfwd.LifecycleEvent;
import com.splunk.cloudfwd.impl.http.HecErrorResponseValueObject;

/**
 * This class is a hack that we use because to determine if HEC token enabled we
 * have to post empty string to /raw and expect this back: {"text":"No
 * data","code":5}. I know. Don't ask. Talk to Itay.
 *
 * @author ghendrey
 */
public class NoDataEventPostResponse extends Response {

    public NoDataEventPostResponse(HecErrorResponseValueObject v,
            LifecycleEvent.Type type, String resp, int httpCode, String url) {
        super(type, 400, resp, url);
        if (v.getCode() != 5 || 400 != httpCode) {
            throw new IllegalStateException(
                    "Sorry, the data provided doesn't represent a NoDataEventPostResponse,");
        }
    }

    /**
     *
     *
     * @return true because this class is just a marker that is used when we
     * post empty string to /raw for token-enabled checks
     */
    @Override
    public boolean isOK() {
        return true;
    }

}
