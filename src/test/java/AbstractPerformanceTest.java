
import com.splunk.cloudfwd.Event;
import com.splunk.cloudfwd.HecConnectionTimeoutException;
import com.splunk.cloudfwd.PropertyKeys;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
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
public abstract class AbstractPerformanceTest extends AbstractConnectionTest {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractPerformanceTest.class.getName());

  @Override
  protected BasicCallbacks getCallbacks() {
    return new ThroughputCalculatorCallback(getNumEventsToSend());
  }
  @Override
    protected String getTestPropertiesFileName() {
    return "lb.properties"; //try as hard as we can to ignore test.properties and not use it
  }
  
  @Override
  protected Properties getProps() {
    Properties props = new Properties(); //default behavior is no "hard coded" test-specific properties
        //the assumption here is that we are doing performance testing using lb.properties not test.properties
    props.put(AbstractConnectionTest.KEY_ENABLE_TEST_PROPERTIES, false);    
    //NOT http mock, but real server is the assumption for these tests
    props.put(PropertyKeys.MOCK_HTTP_KEY, "false");
    return props;
  }  

  @Override
  protected void sendEvents() throws InterruptedException, HecConnectionTimeoutException {
    LOG.trace("SENDING EVENTS WITH CLASS GUID: " + TEST_CLASS_INSTANCE_GUID
            + "And test method GUID " + testMethodGUID);
    int expected = getNumEventsToSend();
    long start = 0;
    long finish = 0;
    float warmup = 0.01f;
    boolean windingDown;
    boolean warmingUp = true;
    for (int i = 0; i < expected; i++) {
      ///final EventBatch events =nextEventBatch(i+1);
      Event event = nextEvent(i + 1);
      long sent = connection.send(event);
      if (sent > 0) {
        warmingUp = (((float) i) / expected) < warmup;
        if (warmingUp) {
          LOG.info("WARMING UP");
        }
        windingDown = (((float) i) / expected) > (1 - warmup);
        if (windingDown) {
          LOG.info("WINDING DOWN");
        }
        if (start == 0L && !warmingUp) { //true first time we exit the warmup period and enter valid sampling period
          start = System.currentTimeMillis(); //start timing after warmup
        }
        if (finish == 0L && windingDown) {
          finish = System.currentTimeMillis();
        }
        if (!warmingUp && !windingDown) {
          ((ThroughputCalculatorCallback) super.callbacks).deferCountUntilAck(
                  event.getId(), sent);
          showThroughput(System.currentTimeMillis(), start);
        }
        LOG.trace("Sent event batch with id: " + event.getId() + " i=" + i + " and size " + sent);
      }

    }
    connection.close(); //will flush
    this.callbacks.await(10, TimeUnit.MINUTES);
    showThroughput(finish, start);
    if (callbacks.isFailed()) {
      Assert.fail(callbacks.getFailMsg());
    }
  }

  private void showThroughput(long finish, long start) {
    //throughput is computed from the currently *acknowledged* size
    long nChars = ((ThroughputCalculatorCallback) super.callbacks).
            getAcknowledgedSize();
    if (finish == start) {
      return;
    }
    if (finish < start) {
      throw new RuntimeException(
              "finish less than start: finish=" + finish + " start=" + start);
    }
    long time = finish - start; //ms
    float sec = ((float) time) / 1000f;
    if (sec <= 0) {
      return;
    }
    LOG.info("Sent " + nChars + " chars in " + time + " ms");
    LOG.info("Chars-per-second: " + nChars / sec);
    float mbps = ((float) nChars * 8) / (sec * 1000000f);
    LOG.info("mbps: " + mbps);
    if (mbps < 0) {
      throw new IllegalStateException("Negative throughput is not allowed");
    }
  }
}
