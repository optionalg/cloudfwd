package com.splunk.cloudfwd.test.mock;

import com.splunk.cloudfwd.ConnectionCallbacks;
import com.splunk.cloudfwd.Connections;
import com.splunk.cloudfwd.EventBatch;
import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.test.util.AbstractConnectionTest;
import com.splunk.cloudfwd.test.util.BasicCallbacks;

import static com.splunk.cloudfwd.PropertyKeys.BLOCKING_TIMEOUT_MS;
import static com.splunk.cloudfwd.PropertyKeys.MOCK_HTTP_CLASSNAME;

import java.util.ArrayList;
import java.util.Properties;
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
 * @author mescobar
 */
public class OutOfOrderAckIdTest extends AbstractConnectionTest {

    final int n = 10000;
    boolean checkpoint = false;
    
    @Test
    public void testWithCheckpointDisabled() throws InterruptedException{
        this.checkpoint = false;
        createConnection();
        super.sendEvents();
    }
    
    @Test
    public void testWithCheckpointEnabled() throws InterruptedException {
      this.checkpoint = true;
      createConnection();
      super.sendEvents();
    }

    @Override
    public void setUp() {
        this.testMethodGUID = java.util.UUID.randomUUID().toString();
        this.events = new ArrayList<>();
    }

    @Override
    protected Properties getProps() {
        Properties props = new Properties();
        props.put(MOCK_HTTP_CLASSNAME,
            "com.splunk.cloudfwd.impl.sim.errorgen.acks.OutOfOrderAckIDEndpoints");
        props.put(BLOCKING_TIMEOUT_MS, "30000");
        props.put(PropertyKeys.UNRESPONSIVE_MS, "-1"); //no dead channel detection
        props.put(PropertyKeys.MAX_TOTAL_CHANNELS, "2");
        props.put(PropertyKeys.ACK_TIMEOUT_MS, "60000"); //we don't want the ack timout kicking in
        // checkpointing
        props.put(PropertyKeys.ENABLE_CHECKPOINTS, Boolean.toString(this.checkpoint));

        return props;
    }

    // Need to separate this logic out of setUp() so that each Test
    // can use different simulated endpoints
    protected void createConnection() {
        this.callbacks = getCallbacks();

        Properties props = new Properties();
        props.putAll(getTestProps());
        props.putAll(getProps());
        this.connection = Connections.create((ConnectionCallbacks) callbacks, props);
        configureConnection(connection);
    }

    @Override
    protected int getNumEventsToSend() {
        return n;
    }

    @Override
    protected BasicCallbacks getCallbacks() {
      if (this.checkpoint) 
        return super.getCallbacks();
      else
        return new IgnoreCheckpointCallbacks(n);
    }
    
    private class IgnoreCheckpointCallbacks extends BasicCallbacks {
        public IgnoreCheckpointCallbacks(int expected) {
            super(expected);
        }

        @Override
        public void checkpoint(EventBatch events) {
          return;
            //noop
        }

        @Override
        public void acknowledged(EventBatch events) {
            if (!acknowledgedBatches.add(events.getId())) {
                Assert.fail(
                        "Received duplicate acknowledgement for event batch:" + events.
                        getId());
                latch.countDown();
            }
            if (acknowledgedBatches.size() == getNumEventsToSend()) {
                latch.countDown();
            }
        }

    }

}
