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
package com.splunk.cloudfwd.impl.http.httpascync;

import com.splunk.cloudfwd.impl.http.HecIOManager;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;

/**
 * This class is responsible for gating until both the ack poll and health poll have returned. This is needed because neither
 * the health endpoint nor the ack endpoints provide, by themselves, all the information needed to determine if HEC is 
 * properly configured.
 * @author ghendrey
 */
public abstract class MultiResponseHandler extends HttpCallbacksAbstract{
    private CountDownLatch latch;

    public MultiResponseHandler(HecIOManager m, HttpCallbacksAbstract... handlers) {
        super(m);
        this.latch = new CountDownLatch(handlers.length);
    }
    
    
    
    void awaitHealthAndAckResp() throws InterruptedException{
        latch.await();
    }
    
}
