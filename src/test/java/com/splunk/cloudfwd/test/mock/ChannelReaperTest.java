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

import com.splunk.cloudfwd.ConnectionSettings;
import com.splunk.cloudfwd.Event;
import com.splunk.cloudfwd.HecHealth;
import com.splunk.cloudfwd.error.HecAcknowledgmentTimeoutException;
import com.splunk.cloudfwd.impl.sim.errorgen.slow.SlowEndpoints;
import com.splunk.cloudfwd.impl.util.HecChannel;
import com.splunk.cloudfwd.test.util.AbstractConnectionTest;
import com.splunk.cloudfwd.test.util.BasicCallbacks;
import org.apache.commons.lang.math.IntRange;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * we set max_total_channels to 1 and channel decommissioning time very small 
 * to make sure that the reaper recreate a channel within that time.
 *
 * @author ghendrey
 */
public class ChannelReaperTest extends AbstractConnectionTest {
    
    @Override
    protected void configureProps(ConnectionSettings settings) {
        settings.setMockHttp(true);
        settings.setMaxTotalChannels(1); //just one channel
        settings.setChannelDecomMS(500, true); //decommission the channel after 500ms  
    }

    @Override
    protected BasicCallbacks getCallbacks() {
        return new BasicCallbacks(getNumEventsToSend()) {
            @Override
            protected boolean isExpectedFailureType(Exception e) {
                LOG.info("isExpectedFailureType: e={}", e);
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
    protected int getNumEventsToSend() { return 0; }

    @Test
    public void sendMessageToJammedChannelThatCannotCloseCleanly()
            throws InterruptedException {
        Optional<HecChannel> channel_id_before = connection.getHealth().stream().map(h -> h.getChannel()).findFirst();
        LOG.info("channel_id_before={}",channel_id_before);
        LOG.info("Before Health={}",connection.getHealth());
        Assert.assertTrue(channel_id_before.isPresent());
        Assert.assertTrue(!channel_id_before.get().getChannelId().isEmpty());
        Thread.sleep(1000); //wait for watchdog
        Optional<HecChannel> channel_id_after = connection.getHealth().stream().map(h -> h.getChannel()).findFirst();
        LOG.info("channel_id_after={}", channel_id_after);
        LOG.info("After Health={}",connection.getHealth());
        Assert.assertTrue(channel_id_after.isPresent());
        Assert.assertTrue(!channel_id_before.get().getChannelId().equals(channel_id_after.get().getChannelId()));
        connection.closeNow(); 
    }

}
