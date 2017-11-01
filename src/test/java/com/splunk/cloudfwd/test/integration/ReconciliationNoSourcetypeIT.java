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
package com.splunk.cloudfwd.test.integration;

import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.Event;
import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import static com.splunk.cloudfwd.test.integration.AbstractReconciliationTest.LOG;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import org.junit.Test;

/**
 *
 * @author ghendrey
 */
public class ReconciliationNoSourcetypeIT extends NoSourcetype{
    
    @Test
    public void sendTextJsonToEventsEndpoint() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        LOG.info("test: sendTextJsonToEventsEndpoint"); 
        connection.getSettings().setHecEndpointType(Connection.HecEndpoint.STRUCTURED_EVENTS_ENDPOINT);
        super.sendCombinationEvents();
        Set<String> searchResults = getEventsFromSplunk();
        verifyResults(getSentEvents(), searchResults);
    }
    
    @Test
    public void sendJsonToEventsEndpoint() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        LOG.info("test: sendJsonToEventsEndpoint"); 
        connection.getSettings().setHecEndpointType(Connection.HecEndpoint.STRUCTURED_EVENTS_ENDPOINT);
        super.eventType = Event.Type.JSON;
        super.sendEvents();
        Set<String> searchResults = getEventsFromSplunk();
        verifyResults(getSentEvents(), searchResults);
    }    

    @Test
    public void sendJsonAsUnvalidatedBytesToEventsEndpoint() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        LOG.info("test: sendJsonAsUnvalidatedBytesToEventsEndpoint");        
        connection.getSettings().setHecEndpointType(Connection.HecEndpoint.STRUCTURED_EVENTS_ENDPOINT);
        super.eventType = Event.Type.UNKNOWN;
        super.sendEvents();
        Set<String> searchResults = getEventsFromSplunk();
        verifyResults(getSentEvents(), searchResults);
    }

    @Test
    public void sendTextToRawEndpoint() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        LOG.info("test: sendTextToRawEndpoint");      
        connection.getSettings().setHecEndpointType(Connection.HecEndpoint.RAW_EVENTS_ENDPOINT);
        super.eventType = Event.Type.TEXT;
        super.sendEvents();
        Set<String> searchResults = getEventsFromSplunk();
        verifyResults(getSentEvents(), searchResults);
    }
    
     @Test
    public void sendTextToEventsEndpoint() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        LOG.info("test: sendTextToEventsEndpoint");  
        connection.getSettings().setHecEndpointType(Connection.HecEndpoint.STRUCTURED_EVENTS_ENDPOINT);
        super.eventType = Event.Type.TEXT;
        super.sendEvents();
        Set<String> searchResults = getEventsFromSplunk();
        verifyResults(getSentEvents(), searchResults);
    }    

    
}
