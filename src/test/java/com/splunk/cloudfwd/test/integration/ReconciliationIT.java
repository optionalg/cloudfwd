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
import com.splunk.cloudfwd.ConnectionSettings;
import com.splunk.cloudfwd.Event;
import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import java.util.*;

import com.splunk.cloudfwd.impl.util.PropertiesFileHelper;
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
public class ReconciliationIT extends AbstractReconciliationTest {
    private String singleLineSourcetype = "__singleline"; //SHOULD_LINEMERGE=false  in props.conf

    public ReconciliationIT() {
    }

    @Test
    public void sendJsonAsUnvalidatedBytesToRawEndpoint() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        connection.getSettings().setHecEndpointType(Connection.HecEndpoint.RAW_EVENTS_ENDPOINT);
        connection.getSettings().setToken(createTestToken(singleLineSourcetype)); // or sourcetype=_json works too
        super.eventType = Event.Type.UNKNOWN;
        super.sendEvents();
        Set<String> searchResults = getEventsFromSplunk();
        verifyResults(getSentEvents(), searchResults);
    }

    @Test
    public void sendJsonAsUnvalidatedBytesToEventsEndpoint() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        connection.getSettings().setHecEndpointType(Connection.HecEndpoint.STRUCTURED_EVENTS_ENDPOINT);
        connection.getSettings().setToken(createTestToken(null));
        super.eventType = Event.Type.UNKNOWN;
        super.sendEvents();
        Set<String> searchResults = getEventsFromSplunk();
        verifyResults(getSentEvents(), searchResults);
    }

    @Test
    public void sendTextToRawEndpoint() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        connection.getSettings().setHecEndpointType(Connection.HecEndpoint.RAW_EVENTS_ENDPOINT);
        connection.getSettings().setToken(createTestToken(null)); // event breaks recognized by timestamp
        super.eventType = Event.Type.TEXT;
        super.sendEvents();
        Set<String> searchResults = getEventsFromSplunk();
        verifyResults(getSentEvents(), searchResults);
    }

    @Test
    public void sendJsonToRawEndpoint() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        connection.getSettings().setHecEndpointType(Connection.HecEndpoint.RAW_EVENTS_ENDPOINT);
        connection.getSettings().setToken(createTestToken(singleLineSourcetype)); // or sourcetype=_json works too
        super.eventType = Event.Type.JSON;
        super.sendEvents();
        Set<String> searchResults = getEventsFromSplunk();
        verifyResults(getSentEvents(), searchResults);
    }

    @Test
    public void sendTextToEventsEndpoint() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        connection.getSettings().setHecEndpointType(Connection.HecEndpoint.STRUCTURED_EVENTS_ENDPOINT);
        connection.getSettings().setToken(createTestToken(null));
        super.eventType = Event.Type.TEXT;
        super.sendEvents();
        Set<String> searchResults = getEventsFromSplunk();
        verifyResults(getSentEvents(), searchResults);
    }

    @Test
    public void sendJsonToEventsEndpoint() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        connection.getSettings().setHecEndpointType(Connection.HecEndpoint.STRUCTURED_EVENTS_ENDPOINT);
        connection.getSettings().setToken(createTestToken(null));
        super.eventType = Event.Type.JSON;
        super.sendEvents();
        Set<String> searchResults = getEventsFromSplunk();
        verifyResults(getSentEvents(), searchResults);
    }

    @Test
    public void sendTextJsonToRawEndpoint() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        connection.getSettings().setHecEndpointType(Connection.HecEndpoint.RAW_EVENTS_ENDPOINT);
        connection.getSettings().setToken(createTestToken(singleLineSourcetype));
        super.sendCombinationEvents();
        Set<String> searchResults = getEventsFromSplunk();
        verifyResults(getSentEvents(), searchResults);
    }

    @Test
    public void sendTextJsonToEventsEndpoint() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        connection.getSettings().setHecEndpointType(Connection.HecEndpoint.STRUCTURED_EVENTS_ENDPOINT);
        connection.getSettings().setToken(createTestToken(null));
        super.sendCombinationEvents();
        Set<String> searchResults = getEventsFromSplunk();
        verifyResults(getSentEvents(), searchResults);
    }

    @Override
    protected void setProps(PropertiesFileHelper settings) {
        settings.setToken(createTestToken(null));
    }

    @Override
    protected int getNumEventsToSend() {
        return numToSend;
    }

}
