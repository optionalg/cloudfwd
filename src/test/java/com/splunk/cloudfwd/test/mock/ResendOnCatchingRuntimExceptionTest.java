package com.splunk.cloudfwd.test.mock;

import com.splunk.cloudfwd.Event;
import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import com.splunk.cloudfwd.PropertyKeys;
import static com.splunk.cloudfwd.PropertyKeys.MOCK_HTTP_KEY;
import static com.splunk.cloudfwd.PropertyKeys.MOCK_HTTP_CLASSNAME;
import com.splunk.cloudfwd.impl.sim.errorgen.runtimeexceptions.ExceptionsEndpoint;
import com.splunk.cloudfwd.test.util.AbstractConnectionTest;
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
 *
 * @author ghendrey
 */
public class ResendOnCatchingRuntimExceptionTest extends AbstractConnectionTest {

  @Override
  protected Properties getProps() {
    Properties props = new Properties();
    props.put(MOCK_HTTP_KEY, "true");
    props.put(MOCK_HTTP_CLASSNAME,
            "com.splunk.cloudfwd.impl.sim.errorgen.runtimeexceptions.ExceptionsEndpoint");
    props.put(PropertyKeys.EVENT_BATCH_SIZE, "0"); //make sure no batching
    props.put(PropertyKeys.MAX_TOTAL_CHANNELS, "1"); //so we insure we resend on same channel   
    //in the ExceptionsEndpoint, error prob is 0.5. So 20 gives us 1/2^20  chance of never getting the message through
    props.put(PropertyKeys.RETRIES, "20");
    return props;
  }

  @Override
  protected int getNumEventsToSend() {
    return 1000;
  }

  protected void sendEvents() throws InterruptedException, HecConnectionTimeoutException {
    int exceptionCount = 0;
    AbstractConnectionTest.LOG.trace(
            "SENDING EVENTS WITH CLASS GUID: " + AbstractConnectionTest.TEST_CLASS_INSTANCE_GUID
            + "And test method GUID " + testMethodGUID);
    int expected = getNumEventsToSend();
    for (int i = 0; i < expected; i++) {
      ///final EventBatchImpl events =nextEventBatch(i+1);
      Event event = nextEvent(i + 1);
      AbstractConnectionTest.LOG.trace("Send event {} i={}", event.getId(), i);
      while (true) {
        try {
          connection.send(event);
          break;
        } catch (ExceptionsEndpoint.ExceptionsEndpointRuntimeException e) {
          exceptionCount++;
          continue;
        }
      }
    }
    Assert.assertTrue("Didn't catch any ExceptionsEndpointRuntimeException",exceptionCount > 0);
    connection.close(); //will flush
    this.callbacks.await(10, TimeUnit.MINUTES);
    if (callbacks.isFailed()) {
      Assert.fail(
              "There was a failure callback with exception class  " + callbacks.
              getException() + " and message " + callbacks.getFailMsg());
    }
  }
  
  @Test
  public void testResendingAsManyTimesAsNeededAndAllShouldSucceed() throws InterruptedException, HecConnectionTimeoutException{
    sendEvents();
  }
  
}
