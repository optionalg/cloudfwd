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
import com.splunk.cloudfwd.error.HecAcknowledgmentTimeoutException;
import com.splunk.cloudfwd.impl.sim.errorgen.slow.SlowEndpoints;
import com.splunk.cloudfwd.impl.util.PropertiesFileHelper;
import com.splunk.cloudfwd.test.util.AbstractConnectionTest;
import com.splunk.cloudfwd.test.util.BasicCallbacks;
import java.util.List;
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
    protected void setProps(PropertiesFileHelper settings) {
        settings.setMockHttp(true);
        settings.setMockHttpClassname("com.splunk.cloudfwd.impl.sim.errorgen.slow.SlowEndpoints");
        settings.setEventBatchSize(0); //make sure no batching
        settings.setMaxRetries(1); //so we insure we resend on same channel  
        settings.setChannelDecomMS(500); //decommission the channel after 500ms  
        settings.setAckTimeoutMS(250); //time out ack in 250ms
        settings.setChannelQuiesceTimeoutMS(750); //watchdog to kill the channel in 750ms
        settings.setMaxUnackedEventBatchPerChannel(1);
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
        SlowEndpoints.sleep = 1000; //endpoint won't respond for 10 seconds
        LOG.info(connection.getHealth().toString());
        super.sendEvents();
        LOG.info(connection.getHealth().toString());
        LOG.info("Sorry: this test needs to wait 3 minutes... :-(");
        Thread.sleep(1500);
        List<HecHealth> healths = connection.getHealth();    
        LOG.info(healths.toString());
        Assert.assertEquals("Expected no channels, but found " + healths, 0,
                connection.getHealth().size()); //the watchdog should have removed this channel
    }

}
