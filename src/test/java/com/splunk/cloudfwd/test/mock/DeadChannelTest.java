package com.splunk.cloudfwd.test.mock;

import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.ConnectionSettings;
import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import com.splunk.cloudfwd.error.HecChannelDeathException;
import com.splunk.cloudfwd.test.util.AbstractConnectionTest;
import com.splunk.cloudfwd.test.util.BasicCallbacks;
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
 * Uses a simulated endpoint that looses acks to make a channel appear dead. Test passes if it receives HecChannelDeathException.
 * @author ghendrey
 */
public class DeadChannelTest extends AbstractConnectionTest {

    protected BasicCallbacks getCallbacks() {
        return new BasicCallbacks(getNumEventsToSend()) {
            @Override
            protected boolean isExpectedWarningType(Exception e) {
                return e instanceof HecChannelDeathException;
            }

            @Override
            public boolean shouldWarn() {
                return true;
            }
        };
    }

  @Override
  protected void configureProps(ConnectionSettings settings) {
    settings.setMockHttp(true);
    settings.setMockHttpClassname("com.splunk.cloudfwd.impl.sim.errorgen.ackslost.LossyEndpoints");
    settings.setUnresponsiveMS(4000);
    settings.setMaxTotalChannels(2);
  }
  
  @Override
  protected void configureConnection(Connection connection) {
    connection.getSettings().setEventBatchSize(0);
    //must be polling for acks fast enough to have responses before we determine channel is dead or alive.
    Assert.assertTrue("Test misconfigured: ack poll interval not less than channel_unresponsive_decom_ms", connection.getSettings().getUnresponsiveChannelDecomMS() > connection.getSettings().getAckPollMS());
  }
  @Override
  protected int getNumEventsToSend() {
    return 10000;
  }
  
  @Test
  public void testDeadChannel() throws TimeoutException, InterruptedException, HecConnectionTimeoutException{
    super.sendEvents();
  }
}
