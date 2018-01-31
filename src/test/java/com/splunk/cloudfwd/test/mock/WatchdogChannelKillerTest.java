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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.splunk.cloudfwd.ConnectionSettings;
import com.splunk.cloudfwd.HecHealth;
import com.splunk.cloudfwd.error.HecAcknowledgmentTimeoutException;
import com.splunk.cloudfwd.impl.ConnectionImpl;
import com.splunk.cloudfwd.impl.sim.errorgen.slow.SlowEndpoints;
import com.splunk.cloudfwd.test.util.AbstractConnectionTest;
import com.splunk.cloudfwd.test.util.BasicCallbacks;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

/**
 * In this test we send one event to a channel whose ack's are very slow. The
 * channel's close should close the channel but the ack never comes back, 
 * so the channel cannot gracefully close. There is no dead channel detector. 
 * So after the channel should be force closed by the watchdog that gets started 
 * on close. 
 *
 * @author ghendrey
 */
public class WatchdogChannelKillerTest extends AbstractConnectionTest {
    
    @Override
    protected void configureProps(ConnectionSettings settings) {
        settings.setMockHttp(true);
        settings.setMockHttpClassname("com.splunk.cloudfwd.impl.sim.errorgen.slow.SlowEndpoints");
        settings.setEventBatchSize(0); // send the event immediately
        settings.setMaxTotalChannels(1); // just one channel
        settings.setAckTimeoutMS(10000); //time out ack in 2000ms
        settings.setChannelQuiesceTimeoutMS(500, true); //watchdog to kill the channel in 750ms
        settings.setMaxUnackedEventBatchPerChannel(1);
    }

    @Override
    protected BasicCallbacks getCallbacks() {
        return new BasicCallbacks(getNumEventsToSend()) {
            
            // we disable await callback to check earlier that 
            // even failure on timeout occurs. 
            @Override public void await(long timeout, TimeUnit u) throws InterruptedException {
                return;
            }
        };
    }

    @Override
    protected int getNumEventsToSend() { return 1; }

    @Test
    public void sendMessageToJammedChannelThatCannotCloseCleanly()
            throws InterruptedException {
        SlowEndpoints.sleep = 10000; //endpoint won't respond for 1 second
        String channel_id_before = connection.getHealth().stream().map(h -> h.getChannel()).findFirst().toString();
        LOG.info("channel_id_before={}",channel_id_before);
        LOG.info("Before Health={}",connection.getHealth());
        super.sendEvents(true, false); //send the one event, then procede immediately to the next line
        Thread.sleep(1000); //wait for reaper to replace a channel
        connection.close(); //channel will never quiesce, so watchdog must kick in to close it
        Thread.sleep(1000); //wait for reaper to replace a channel
        String channel_id_after = connection.getHealth().stream().map(h -> h.getChannel()).findFirst().toString();
        LOG.info("channel_id_after={}", channel_id_after);
        LOG.info("After Health={}",connection.getHealth());
        Assert.assertTrue( // we check that the channel was successfully closed
                "Watchdog have not killed channel=" + channel_id_after,
                connection.getHealth().isEmpty()); 
        connection.closeNow();
    }
}
