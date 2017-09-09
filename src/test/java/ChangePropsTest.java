
import com.splunk.cloudfwd.*;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
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
public class ChangePropsTest extends AbstractConnectionTest {
    private int numEvents = 100000;
    private int numPropertyChanges = 2;
    String[] tokens = {"4B521A0D-C5D8-4721-B68E-B4947E4CD33A", "EB610A08-D40A-4B7C-A67D-18E9E75570BE" };


    @Test
    public void changePropertiesTest() throws InterruptedException, HecConnectionTimeoutException {
        sendEvents();
        close();
    }

    private void close() throws InterruptedException {
    }

    @Override
    protected void sendEvents() throws InterruptedException, HecConnectionTimeoutException {
        Assert.assertFalse("Connection should not be closed.", connection.isClosed());
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
            if (i < numPropertyChanges) connection.setProperties(changeProps(i));
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

    private Properties changeProps(int i) {
        Properties props = new Properties();
        props.putAll(getTestProps());
//        props.put(PropertyKeys.EVENT_BATCH_SIZE, Integer.toString(eventBatchSize));
//        props.put(PropertyKeys.BLOCKING_TIMEOUT_MS, Integer.toString(blockingTimeout));
        props.put(PropertyKeys.TOKEN, tokens[i]);
        return props;
    }

    @Override
    protected int getNumEventsToSend() {
        return numEvents;
    }

    protected BasicCallbacks getCallbacks() {
        return new BasicCallbacks(getNumEventsToSend() * (numPropertyChanges+1));
    }


}
