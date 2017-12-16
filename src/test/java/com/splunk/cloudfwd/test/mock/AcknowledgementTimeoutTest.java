package com.splunk.cloudfwd.test.mock;/*
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

import com.splunk.cloudfwd.ConnectionSettings;
import com.splunk.cloudfwd.Event;
import com.splunk.cloudfwd.EventBatch;
import com.splunk.cloudfwd.error.HecAcknowledgmentTimeoutException;
import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import com.splunk.cloudfwd.test.util.AbstractConnectionTest;
import com.splunk.cloudfwd.test.util.BasicCallbacks;
import com.splunk.cloudfwd.impl.sim.errorgen.slow.SlowEndpoints;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author ghendrey
 */
public class AcknowledgementTimeoutTest extends AbstractConnectionTest {

    private static final Logger LOG = LoggerFactory.getLogger(
            AcknowledgementTimeoutTest.class.getName());

    public AcknowledgementTimeoutTest() {}

    @Override
    @Before
    public void setUp() {
        super.setUp();
    }
    
    @After
    @Override
    public void tearDown() {
        super.tearDown();
        connection.closeNow();
    }    

    @Test
    public void testTimeout() throws InterruptedException, HecConnectionTimeoutException {
        super.eventType = Event.Type.TEXT;
        sendEvents();
    }

    @Override
    protected void configureProps(ConnectionSettings settings) {
        // props.put(ConnectionSettings.MOCK_HTTP_KEY, "true");
        //simulate a slow endpoint
        settings.setMockHttpClassname("com.splunk.cloudfwd.impl.sim.errorgen.slow.SlowEndpoints");
        if (SlowEndpoints.sleep > 10000) {
            throw new RuntimeException("Let's not get carried away here");
        }
        settings.setAckTimeoutMS(1000);
        settings.setUnresponsiveMS(-1); //disable dead channel detection
        settings.setEventBatchSize(0);
        //props.put(PropertyKeys.MAX_TOTAL_CHANNELS, 1);
    }

    @Override
    protected int getNumEventsToSend() {
        return 1;
    }

    @Override
    protected void sendEvents() throws InterruptedException, HecConnectionTimeoutException {
        super.sendEvents();      
    }

    @Override
    protected BasicCallbacks getCallbacks() {
        return new TimeoutCatcher(getNumEventsToSend());
    }

    class TimeoutCatcher extends BasicCallbacks {

        public TimeoutCatcher(int expected) {
            super(expected);
        }
   
        protected boolean isExpectedFailureType(Exception e){
          return e instanceof HecAcknowledgmentTimeoutException;
        }

          /**
           * subclass musts override to return true when their test generates an expected exception
           * @return
           */
          public boolean shouldFail(){
            return true;
        }
        
        

        @Override
        public void checkpoint(EventBatch events) {
            LOG.trace("SUCCESS CHECKPOINT " + events.getId());
            Assert.fail(
                    "Got an unexpected checkpoint when we were waiting for timeout: " + events);
        }

        @Override
        public void acknowledged(EventBatch events) {
            Assert.fail(
                    "Got an unexpected acknowledged when we were waiting for timeout: "+ events);

        }
    }
}
