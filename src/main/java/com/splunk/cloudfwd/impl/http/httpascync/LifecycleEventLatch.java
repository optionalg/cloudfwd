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

import com.splunk.cloudfwd.LifecycleEvent;
import java.util.concurrent.CountDownLatch;

/**
 * A latch that contains a LifecycleState responsible for decrementing the count.
 * @author ghendrey
 */
public class LifecycleEventLatch extends CountDownLatch{

    private LifecycleEvent lifecycleEvent;
    
    public LifecycleEventLatch(int count) {
        super(count);
    }
    
    public void countDown(LifecycleEvent e){
        this.lifecycleEvent = e;
        super.countDown();
    }
    
    @Override
    public void countDown(){
        throw new IllegalStateException("countDown is off limits. Use the variant that accepts a LifecycleEvent.");
    }

    /**
     * @return the lifecycleEvent
     */
    public LifecycleEvent getLifecycleEvent() {
        return lifecycleEvent;
    }
    
}
