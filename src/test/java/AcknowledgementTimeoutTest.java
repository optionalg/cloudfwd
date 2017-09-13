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

import com.splunk.cloudfwd.Event;
import com.splunk.cloudfwd.EventBatch;
import com.splunk.cloudfwd.HecAcknowledgmentTimeoutException;
import com.splunk.cloudfwd.HecConnectionTimeoutException;
import static com.splunk.cloudfwd.PropertyKeys.*;
import com.splunk.cloudfwd.sim.errorgen.slow.SlowEndpoints;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author ghendrey
 */
public class AcknowledgementTimeoutTest extends AbstractConnectionTest {
  private static final Logger LOG = LoggerFactory.getLogger(AcknowledgementTimeoutTest.class.getName());

  public AcknowledgementTimeoutTest() {
  }

  @Override
  @Before
  public void setUp() {
    super.setUp();
    super.connection.setAckTimeoutMS(100);
  }

  @Test
  public void testTimeout() throws InterruptedException, HecConnectionTimeoutException {
    
      super.eventType = Event.Type.TEXT;
      super.sendEvents();

  }

  @Override
  protected Properties getProps() {
    Properties props = new Properties();
    // props.put(PropertiesFileHelper.MOCK_HTTP_KEY, "true");
    //simulate a slow endpoint
    props.put(MOCK_HTTP_CLASSNAME,
            "com.splunk.cloudfwd.sim.errorgen.slow.SlowEndpoints");
    if(SlowEndpoints.sleep > 10000){
      throw new RuntimeException("Let's not get carried away here");
    }
    //timeout must be less than the slow endpoint sleep
    long timeout = SlowEndpoints.sleep/2;
    if(timeout < 1000){
      throw  new RuntimeException("Test timeout too low to be reliable");
    }
    props.put(ACK_TIMEOUT_MS, timeout);
    props.put(UNRESPONSIVE_MS,
            "-1");//disable dead channel detection

    return props;
  }

  @Override
  protected int getNumEventsToSend() {
    return 1;
  }
  
  @Override
  protected void sendEvents() throws InterruptedException, HecConnectionTimeoutException {
    super.sendEvents();
    Assert.assertTrue("didn't get expected timeout",((TimeoutCatcher)super.callbacks).gotTimeout);
  }

  @Override
  protected BasicCallbacks getCallbacks() {
    return new TimeoutCatcher(getNumEventsToSend());
  }
  
  class TimeoutCatcher extends BasicCallbacks{
    public boolean gotTimeout;

    public TimeoutCatcher(int expected) {
      super(expected);
    }
   @Override
      public void failed(EventBatch events, Exception e) {
        //We expect a timeout
        Assert.assertTrue(e.getMessage(), e instanceof HecAcknowledgmentTimeoutException);
        LOG.trace("Got expected exception: " + e);
        if(e instanceof TimeoutException){
          gotTimeout = true;
        }
        latch.countDown(); //allow the test to finish
      }
  }
}
