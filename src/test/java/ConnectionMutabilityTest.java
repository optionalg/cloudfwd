
import com.splunk.cloudfwd.*;

import java.util.*;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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

/**
 * @author eprokop
 */
// TODO: tests for the 4 cases things amazon wants to be able to change on the fly: token, url, endpoint type (already in API?), ack_timeout_ms
public class ConnectionMutabilityTest extends AbstractConnectionTest {
    private int numEvents = 200000;
    private int numPropertyChanges;
    protected List<Map<String, List<String>>> propsToChange; // Map: Property name -> List of values to change

    // make this abstract. each test should override
    protected void setPropsToChange() {
        // tokens
        Map tokens = new HashMap<String, List<String>>();
        List<String> tokenValues = new ArrayList<>();
        tokenValues.add("4B521A0D-C5D8-4721-B68E-B4947E4CD33A");
        tokenValues.add("EB610A08-D40A-4B7C-A67D-18E9E75570BE");
        tokenValues.add("4B521A0D-C5D8-4721-B68E-B4947E4CD33A");
        tokenValues.add("EB610A08-D40A-4B7C-A67D-18E9E75570BE");
        tokenValues.add("4B521A0D-C5D8-4721-B68E-B4947E4CD33A");
        tokens.put(PropertyKeys.TOKEN, tokenValues);
        propsToChange.add(tokens);
    }

    protected int getNumPropertyChanges() {
        setPropsToChange();
        int num = 0;
        for (Map<String, List<String>> m : propsToChange) {
            for (Collection<String> list : m.values()) {
                if (list.size() > num) {
                    num = list.size();
                }
            }
        }
        return num;
    }

    @Test
    public void changePropertiesTest() throws InterruptedException, HecConnectionTimeoutException {
        sendEvents();
        close();
    }

    private void close() throws InterruptedException {
    }

    // Sends numEvents. Changes the properties. Sends numEvents.
    // Changes the properties. Sends numEvents. etc.
    @Override
    protected void sendEvents() throws InterruptedException, HecConnectionTimeoutException {
        System.out.println(
                "SENDING EVENTS WITH CLASS GUID: " + TEST_CLASS_INSTANCE_GUID
                        + "And test method GUID " + testMethodGUID);
        int start = 0;
        int stop = getNumEventsToSend();
        for (int i = 0; i <= numPropertyChanges; i++) {
            for (int j = start; j < stop; j++) {
                Event event = nextEvent(j + 1);
                System.out.println("Send event: " + event.getId() + " j=" + j);
                connection.send(event);
                checkProperties();
            }
            start = stop;
            stop += getNumEventsToSend();
            if (i < numPropertyChanges) connection.setProperties(nextProps(i));
            Assert.assertFalse("Connection should not be closed.", connection.isClosed());
        }
        connection.close(); //will flush
        this.callbacks.await(10, TimeUnit.MINUTES);
        if (callbacks.isFailed()) {
            Assert.fail("There was a failure callback with exception class  " + callbacks.getException() + " and message " + callbacks.getFailMsg());
        }

    }

    private void checkProperties() {
//        Assert.assertEquals("Properties should have changed.", connection.getEventBatchSize(), )
    }

    private Properties nextProps(int i) {
        Properties props = new Properties();
        props.putAll(getTestProps());
        for (Map<String, List<String>> m : propsToChange) {
            for (String name : m.keySet()) {
                if (m.get(name).size() > i) {
                    props.put(name, m.get(name).get(i));
                }
            }
        }
        return props;
    }

    @Override
    @Before
    public void setUp() {
        propsToChange = new ArrayList<>();
        setPropsToChange();
        numPropertyChanges = getNumPropertyChanges();
        this.callbacks = getCallbacks();
        Properties props = new Properties();
        props.putAll(getTestProps());
        props.putAll(getProps());
        this.connection = Connection.createConnection((ConnectionCallbacks) callbacks, props);
        configureConnection(connection);
        this.testMethodGUID = java.util.UUID.randomUUID().toString();
        this.events = new ArrayList<>();
    }

    @Override
    protected int getNumEventsToSend() {
        return numEvents;
    }

    protected BasicCallbacks getCallbacks() {
        return new BasicCallbacks(getNumEventsToSend() * (numPropertyChanges+1));
    }
}
