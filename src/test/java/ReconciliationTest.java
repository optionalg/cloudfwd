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
import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.Event;
import com.splunk.cloudfwd.HecConnectionTimeoutException;

import java.util.*;

import org.junit.Test;
import java.util.concurrent.TimeoutException;


/**
 * NOTES:
 * - Each event sent in a test MUST have unique content for the test to pass,
 *      (add a sequence number in the event text).
 * - Configure the sourcetype in Splunk to break on every newline so events are
 *      indexed properly (SHOULD_LINEMERGE=false in props.conf)
 * - Make sure the configuration (below) is synced to the Splunk search head – otherwise
 *      search results will be empty and the test will fail.
 *
 * @author eprokop
 */
public class ReconciliationTest extends AbstractReconciliationTest {


    public ReconciliationTest() {
    }
    
    @Test
    public void sendJsonAsUnvalidatedBytesToRawEndpoint() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        connection.setHecEndpointType(Connection.HecEndpoint.RAW_EVENTS_ENDPOINT);
        super.eventType = Event.Type.UNKNOWN;
        super.sendEvents();
        Set<String> searchResults = getEventsFromSplunk();
        verifyResults(getSentEvents(), searchResults);
    }    

    @Test
    public void sendJsonAsUnvalidatedBytesToEventsEndpoint() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        connection.setHecEndpointType(Connection.HecEndpoint.STRUCTURED_EVENTS_ENDPOINT);
        super.eventType = Event.Type.UNKNOWN;
        super.sendEvents();
        Set<String> searchResults = getEventsFromSplunk();
        verifyResults(getSentEvents(), searchResults);
    }       
    
    @Test
    public void sendTextToRawEndpoint() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        connection.setHecEndpointType(Connection.HecEndpoint.RAW_EVENTS_ENDPOINT);
        super.eventType = Event.Type.TEXT;
        super.sendEvents();
        Set<String> searchResults = getEventsFromSplunk();
        verifyResults(getSentEvents(), searchResults);
    }

    @Test
    public void sendJsonToRawEndpoint() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        connection.setHecEndpointType(Connection.HecEndpoint.RAW_EVENTS_ENDPOINT);
        super.eventType = Event.Type.JSON;
        super.sendEvents();
        Set<String> searchResults = getEventsFromSplunk();
        verifyResults(getSentEvents(), searchResults);
    }

    @Test
    public void sendTextToEventsEndpoint() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        connection.setHecEndpointType(Connection.HecEndpoint.STRUCTURED_EVENTS_ENDPOINT);
        super.eventType = Event.Type.TEXT;
        super.sendEvents();
        Set<String> searchResults = getEventsFromSplunk();
        verifyResults(getSentEvents(), searchResults);
    }

    @Test
    public void sendJsonToEventsEndpoint() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        connection.setHecEndpointType(Connection.HecEndpoint.STRUCTURED_EVENTS_ENDPOINT);
        super.eventType = Event.Type.JSON;
        super.sendEvents();
        Set<String> searchResults = getEventsFromSplunk();
        verifyResults(getSentEvents(), searchResults);
    }

    @Test
    public void sendTextJsonToRawEndpoint() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        connection.setHecEndpointType(Connection.HecEndpoint.RAW_EVENTS_ENDPOINT);
        super.sendCombinationEvents();
        Set<String> searchResults = getEventsFromSplunk();
        verifyResults(getSentEvents(), searchResults);
    }

    @Test
    public void sendTextJsonToEventsEndpoint() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        connection.setHecEndpointType(Connection.HecEndpoint.STRUCTURED_EVENTS_ENDPOINT);
        super.sendCombinationEvents();
        Set<String> searchResults = getEventsFromSplunk();
        verifyResults(getSentEvents(), searchResults);
    }

    @Override
    protected int getNumEventsToSend() {
        return numToSend;
    }

}
