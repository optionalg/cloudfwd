
import com.splunk.cloudfwd.EventBatch;
import com.splunk.cloudfwd.util.PropertiesFileHelper;
import com.splunk.cloudfwd.sim.errorgen.unhealthy.TriggerableUnhealthyEndpoints;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;
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
public final class UnhealthyEndpointTest extends AbstractConnectionTest {

  private final UnhealthyCallbackDetector customCallback;

  public UnhealthyEndpointTest() {
    customCallback = new UnhealthyCallbackDetector(getNumEventsToSend());
  }

  @Test
  public void testHealthyThenUnhealthyThenHealthy() throws TimeoutException, InterruptedException {
    sendEvents();
  }

  @Override
  protected Properties getProps() {
    Properties props = new Properties();
    props.put(PropertiesFileHelper.MOCK_HTTP_KEY, "true");
    //simulate a non-sticky endpoint
    props.put(PropertiesFileHelper.MOCK_HTTP_CLASSNAME_KEY,
            "com.splunk.cloudfwd.sim.errorgen.unhealthy.TriggerableUnhealthyEndpoints");
    return props;
  }

  @Override
  protected int getNumEventsToSend() {
    return 2;
  }

  @Override
  protected void sendEvents() throws TimeoutException, InterruptedException {
    int expected = getNumEventsToSend();
    TriggerableUnhealthyEndpoints.healthy = true;
    customCallback.expectAck = true;
    connection.send(getTimestampedRawEvent(1)); //shoud acknowledge
    this.callbacks.await(10, TimeUnit.MINUTES); //wait for both messages to ack
    connection.close(); //will flush 
  }

  @Override
  protected BasicCallbacks getCallbacks() {
    return customCallback;
  }

  class UnhealthyCallbackDetector extends BasicCallbacks {

    boolean expectAck = true;
    boolean done = false;

    public UnhealthyCallbackDetector(int expected) {
      super(expected);
    }

    @Override
    public void acknowledged(EventBatch events) {
      if (done) {
        return;
      }
      if (!expectAck) {
        Assert.fail(
                "Got an ack when we expected unhealthy indexer to be blocked");
      } else {
        try {
          expectAck = false;//any acknowledged received will cause test fail
          //MAKE UNhealthy then send a second message
          TriggerableUnhealthyEndpoints.healthy = false;
          Thread.sleep(2000); //make sure health poll becomes unhealthy (poll has interval so we must wait)
          connection.send(getTimestampedRawEvent(2));
          Thread.sleep(2000); //this is definitely enough time to detect the assertion above and fail if we see it         
          done = true; //so we don't get in an infinite loop of sending (which would repeat message 2)
          TriggerableUnhealthyEndpoints.healthy = true; //will unblock the HecChannel on next health poll  
          //...which will cause acknowledged to be invoked again, but now done=true so test will end.
        } catch (InterruptedException ex) {
          Logger.getLogger(UnhealthyEndpointTest.class.getName()).
                  log(Level.SEVERE, null, ex);
        }

      }

    }
  }

}
