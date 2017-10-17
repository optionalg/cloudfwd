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
package com.splunk.cloudfwd.test.mock;

import com.splunk.cloudfwd.HecHealth;
import com.splunk.cloudfwd.PropertyKeys;
import static com.splunk.cloudfwd.PropertyKeys.MOCK_HTTP_CLASSNAME;
import static com.splunk.cloudfwd.PropertyKeys.MOCK_HTTP_KEY;
import com.splunk.cloudfwd.error.HecAcknowledgmentTimeoutException;
import com.splunk.cloudfwd.impl.sim.errorgen.slow.SlowEndpoints;
import com.splunk.cloudfwd.test.util.AbstractConnectionTest;
import com.splunk.cloudfwd.test.util.BasicCallbacks;
import java.util.List;
import java.util.Properties;
import org.junit.Assert;
import org.junit.Test;

/**
 * In this test we send one event to a channel whose ack's are very slow. The
 * channel's reaper should kill quiesce the channel after 5 seconds, but the ack
 * never comes back, so the channel cannot gracefully close. There is no dead
 * channel detector. So in 3 minutes the channel should be force closed by the
 * watchdog that gets started on close. Close will happen immediately after the
 * first send.
 *
 * @author ghendrey
 */
public class WatchdogChannelKillerTest extends AbstractConnectionTest {

    @Override
    protected Properties getProps() {
        Properties props = new Properties();
        props.setProperty(MOCK_HTTP_KEY, "true");
        props.setProperty(MOCK_HTTP_CLASSNAME,
                "com.splunk.cloudfwd.impl.sim.errorgen.slow.SlowEndpoints");
        props.setProperty(PropertyKeys.EVENT_BATCH_SIZE, "0"); //make sure no batching
        props.setProperty(PropertyKeys.MAX_TOTAL_CHANNELS, "1"); //so we insure we resend on same channel  
        props.setProperty(PropertyKeys.CHANNEL_DECOM_MS, "5000"); //decomission the channel after 5 seconds  
        props.setProperty(PropertyKeys.ACK_TIMEOUT_MS, "1000"); //1 second for ack timeoout
        props.setProperty(PropertyKeys.MAX_UNACKED_EVENT_BATCHES_PER_CHANNEL, "1");       
        return props;
    }

    @Override
    protected BasicCallbacks getCallbacks() {
        return new BasicCallbacks(getNumEventsToSend()) {
            @Override
            protected boolean isExpectedFailureType(Exception e) {
                Assert.assertTrue("Excpected",
                        e instanceof HecAcknowledgmentTimeoutException);
                return true;
            }

            @Override
            public boolean shouldFail() {
                return true;
            }
        };
    }

    @Override
    protected int getNumEventsToSend() {
        return 1;
    }

    @Test
    public void sendMessageToJammedChannelThatCannotCloseCleanly()
            throws InterruptedException {
        SlowEndpoints.sleep = 500000; //endpoint won't respond for 500 seconds
        LOG.info(connection.getHealth().toString());
        super.sendEvents();
        LOG.info(connection.getHealth().toString());
        LOG.info("Sorry: this test needs to wait 3 minutes... :-(");
        Thread.sleep(180000); //wait 3 minutes
        List<HecHealth> healths = connection.getHealth();    
        LOG.info(healths.toString());
        Assert.assertEquals("Expected no channels, but found " + healths, 0,
                connection.getHealth().size()); //the watchdog should have removed this channel
    }

}
