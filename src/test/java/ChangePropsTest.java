
import com.splunk.cloudfwd.*;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
 *
 * @author ghendrey
 */
public class ChangePropsTest extends AbstractConnectionTest {
    private int numEvents = 100;
    Properties props;

    @Test
    public void changePropertiesTest() throws InterruptedException, HecConnectionTimeoutException {
        sendSomeEvents();
        connection.setProperties(
                changeProps(0,15000));
        sendSomeEvents();
        connection.setProperties(
                changeProps(5000,30000));
        sendSomeEvents();
        close();
    }

    private void close() throws InterruptedException {
        connection.close(); //will flush
        this.callbacks.await(10, TimeUnit.MINUTES);
        if (callbacks.isFailed()) {
            Assert.fail("There was a failure callback with exception class  " + callbacks.getException() + " and message " + callbacks.getFailMsg());
        }
    }

    private void sendSomeEvents() throws InterruptedException, HecConnectionTimeoutException {
        Assert.assertFalse("Connection should not be closed.", connection.isClosed());
        System.out.println(
                "SENDING EVENTS WITH CLASS GUID: " + TEST_CLASS_INSTANCE_GUID
                        + "And test method GUID " + testMethodGUID);
        int expected = getNumEventsToSend();
        for (int i = 0; i < expected; i++) {
            Event event = nextEvent(i + 1);
            System.out.println("Send event: " + event.getId() + " i=" + i);
            connection.send(event);
            checkProperties();
        }

    }

    private void checkProperties() {
        Assert.assertEquals("Properties should have changed.", connection.getEventBatchSize(), )
    }

    protected Properties changeProps(int eventBatchSize, int blockingTimeout) {
        props = new Properties();
        props.put(PropertyKeys.EVENT_BATCH_SIZE, Integer.toString(eventBatchSize));
        props.put(PropertyKeys.BLOCKING_TIMEOUT_MS, Integer.toString(blockingTimeout));
        return props;
    }

    @Override
    protected int getNumEventsToSend() {
        return numEvents;
    }

    protected BasicCallbacks getCallbacks() {
        return new BasicCallbacks(5);
    }


}
