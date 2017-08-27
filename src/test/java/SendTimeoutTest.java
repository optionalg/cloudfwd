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
import com.splunk.cloudfwd.util.PropertiesFileHelper;
import com.splunk.cloudfwd.EventBatch;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author ghendrey
 */
public class SendTimeoutTest extends AbstractConnectionTest {

  public SendTimeoutTest() {
  }

  @Override
  @Before
  public void setUp() {
    super.setUp();
    super.connection.setEventAcknowledgementTimeout(100);
  }

  @Test
  public void testTimeout() throws InterruptedException {
    try {
      super.eventType = EventType.TEXT;
      super.sendEvents();
    } catch (TimeoutException ex) {

    }
  }

  @Override
  protected Properties getProps() {
    Properties props = new Properties();
    props.put(PropertiesFileHelper.MOCK_HTTP_KEY, "true");
    //simulate a slow endpoint
    props.put(PropertiesFileHelper.MOCK_HTTP_CLASSNAME_KEY,
            "com.splunk.cloudfwd.sim.errorgen.slow.SlowEndpoints");
    return props;
  }

  @Override
  protected int getNumEventsToSend() {
    return 1;
  }

  @Override
  protected BasicCallbacks getCallbacks() {
    return new BasicCallbacks(getNumEventsToSend()) {
      @Override
      public void failed(EventBatch events, Exception e) {
        //We expect a timeout
        Assert.
                assertTrue(e.getMessage(),
                        e instanceof TimeoutException);
        System.out.println("Got expected exception: " + e);
        latch.countDown(); //allow the test to finish
      }
    };
  }
}
