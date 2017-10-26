package com.splunk.cloudfwd.test.integration;/*
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
import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.Event;
import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import java.util.*;

import org.junit.Test;
import java.util.concurrent.TimeoutException;

/**
 * It is NOT RECOMMENDED to run these tests against a production Splunk instance.
 * See AbstractReconciliationTest for more details.
 *
 * Tests that generic dummy data in text and json form are indexed properly
 * by HEC endpoints. All sample data sent to Splunk is deleted after the
 * tests complete.
 *
 * @author eprokop
 */
public class ReconciliationSingleLineSourcetype1T extends AbstractReconciliationTest {


    public ReconciliationSingleLineSourcetype1T() {
    }

    @Test
    public void sendJsonAsUnvalidatedBytesToRawEndpoint() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        LOG.info("test: sendJsonAsUnvalidatedBytesToRawEndpoint");
        connection.getSettings().setHecEndpointType(Connection.HecEndpoint.RAW_EVENTS_ENDPOINT);
        super.eventType = Event.Type.UNKNOWN;
        super.sendEvents();
        Set<String> searchResults = getEventsFromSplunk();
        verifyResults(getSentEvents(), searchResults);
    }
    
    @Test
    public void sendTextJsonToRawEndpoint() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        LOG.info("test: sendTextJsonToRawEndpoint"); 
        connection.getSettings().setHecEndpointType(Connection.HecEndpoint.RAW_EVENTS_ENDPOINT);
        super.sendCombinationEvents();
        Set<String> searchResults = getEventsFromSplunk();
        verifyResults(getSentEvents(), searchResults);
    }   

        @Test
    public void sendJsonToRawEndpoint() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        LOG.info("test: sendJsonToRawEndpoint");   
        connection.getSettings().setHecEndpointType(Connection.HecEndpoint.RAW_EVENTS_ENDPOINT);
        super.eventType = Event.Type.JSON;
        super.sendEvents();
        Set<String> searchResults = getEventsFromSplunk();
        verifyResults(getSentEvents(), searchResults);
    }



}
