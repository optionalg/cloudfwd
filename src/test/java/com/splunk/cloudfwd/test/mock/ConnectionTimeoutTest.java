package com.splunk.cloudfwd.test.mock;

import com.splunk.cloudfwd.Event;
import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.test.util.AbstractConnectionTest;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class ConnectionTimeoutTest extends AbstractConnectionTest {

  private static final Logger LOG = LoggerFactory.getLogger(ConnectionTimeoutTest.class.getName());

  @Override
  protected Properties getProps() {
    Properties props = new Properties();
    //A realistic value of BLOCKING_TIMEOUT_MS would be 1 or more MINUTES, but let's not
    //make this test run too slowly. The point is, we want to SEE the HecConnectionTimeout
    //happen repeatedly, until the message goes through
    props.put(PropertyKeys.BLOCKING_TIMEOUT_MS, "100"); //block for 100 ms before HecConnectionTimeout
    //install an endpoint that takes 10 seconds to return an ack
    props.put(PropertyKeys.MOCK_HTTP_CLASSNAME,
            "com.splunk.cloudfwd.impl.sim.errorgen.slow.SlowEndpoints");
    props.put(PropertyKeys.MAX_UNACKED_EVENT_BATCHES_PER_CHANNEL, "1");
    props.put(PropertyKeys.MAX_TOTAL_CHANNELS, "1");
    props.put(PropertyKeys.ACK_TIMEOUT_MS, "60000"); //we don't want the ack timout kicking in
    props.put(PropertyKeys.ACK_POLL_MS, "250");
    props.put(PropertyKeys.UNRESPONSIVE_MS, "-1"); //no dead channel detection
    return props;
  }

  @Override
  protected void sendEvents() throws InterruptedException {
    if (getNumEventsToSend() > 2) {
      throw new RuntimeException(
              "This test uses close(), not closeNow(), so don't jam it up with more than one Batch to test on "
                      + "a jammed up channel. It will take too long to be practical.");
    }
    LOG.trace("sendEvents: SENDING EVENTS WITH CLASS GUID: " + AbstractConnectionTest.TEST_CLASS_INSTANCE_GUID
            + "And test method GUID " + testMethodGUID);
    try {
      //send a first message to block the channel (we are using the slowendpoints to jam up the channel, see getProps)
      connection.send(nextEvent(1));
    } catch (HecConnectionTimeoutException ex) {
      Assert.fail(
              "The first message should send - but we got an HecConnectionTimeoutException");
    }
    //it will take about 10 seconds for the SlowEndpoints to ack, and unjam the channel
    int expected = getNumEventsToSend();
    for (int i = 2; i <= expected; i++) { //"-1" because we already sent one of the events (above)
      Event event = nextEvent(i);
      LOG.trace("sendEvents: Send event: " + event.getId() + " i=" + i);
      int connTimeoutCount = 0;
      while (true) {
        try {
          connection.send(event);
          break;
        } catch (HecConnectionTimeoutException e) {
          if (connTimeoutCount++ > 20) {
            //Assert.fail("Too many HecConnectionTimeouts");
            return;
          }
        }
      }
      Assert.assertTrue("Did not get any HecConnectionTimeouts",
              connTimeoutCount != 0);
    }
    connection.close();
    this.callbacks.await(10, TimeUnit.MINUTES);
    if (callbacks.isFailed()) {
      Assert.fail(callbacks.getFailMsg());
    }
  }

  @Override
  protected int getNumEventsToSend() {
    return 2;
  }

  @Test
  public void testHecConnectionTimeout() throws InterruptedException {
    sendEvents();
  }

}
