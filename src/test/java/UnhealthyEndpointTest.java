
import com.splunk.cloudfwd.EventBatch;
import com.splunk.cloudfwd.HecConnectionTimeoutException;
import static com.splunk.cloudfwd.PropertyKeys.*;
import static com.splunk.cloudfwd.PropertyKeys.UNRESPONSIVE_MS;
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
  public void testHealthyThenUnhealthyThenHealthy() throws HecConnectionTimeoutException, InterruptedException {
    sendEvents();
  }

  @Override
  protected Properties getProps() {
    Properties props = new Properties();
    //props.put(PropertiesFileHelper.MOCK_HTTP_KEY, "true");
    //simulate a non-sticky endpoint
    props.put(MOCK_HTTP_CLASSNAME,
            "com.splunk.cloudfwd.sim.errorgen.unhealthy.TriggerableUnhealthyEndpoints");
    props.put(MAX_TOTAL_CHANNELS, "1");
    props.put(MAX_UNACKED_EVENT_BATCHES_PER_CHANNEL,"1000");
    props.put(ACK_POLL_MS, "250");
    props.put(HEALTH_POLL_MS, "250");
    props.put(UNRESPONSIVE_MS, "-1"); //disable dead channel removel

    return props;
  }

  @Override
  protected int getNumEventsToSend() {
    return 2;
  }

  @Override
  protected void sendEvents() throws HecConnectionTimeoutException, InterruptedException {
    int expected = getNumEventsToSend();
    TriggerableUnhealthyEndpoints.healthy = true;
    try {
      connection.send(getTimestampedRawEvent(1)); //shoud acknowledge
    } catch (HecConnectionTimeoutException ex) {
      Logger.getLogger(UnhealthyEndpointTest.class.getName()).
              log(Level.SEVERE, null, ex);
      Assert.fail();
    }
    this.callbacks.await(10, TimeUnit.MINUTES); //wait for both messages to ack
    connection.close(); //will flush 
  }

  @Override
  protected BasicCallbacks getCallbacks() {
    return customCallback;
  }

  class UnhealthyCallbackDetector extends BasicCallbacks {

    long sleepTime = 3000; //3 sec
    int count = 0;

    public UnhealthyCallbackDetector(int expected) {
      super(expected);
      TriggerableUnhealthyEndpoints.healthy = true;
    }

    @Override
    public void acknowledged(EventBatch events) {
      count++;
      if (count == 2) {
        Assert.assertTrue("Message Failed to block on unhealthy channel",
                TriggerableUnhealthyEndpoints.healthy);
        return; //need one event to return so that we start polling
      }

      try {
        System.out.println("Got first ack");
        //MAKE UNhealthy then send a second message
        TriggerableUnhealthyEndpoints.healthy = false;
        System.out.println("waiting to detect unhealthy channel");
        Thread.sleep(sleepTime); //make sure health poll becomes unhealthy (poll has interval so we must wait)
        System.out.println("sending event that we expect to block on send");
        //must send from another thread
        new Thread(() -> {
          long start = System.currentTimeMillis();
          try {
            connection.send(getTimestampedRawEvent(2));
          } catch (HecConnectionTimeoutException ex) {
            Logger.getLogger(UnhealthyEndpointTest.class.getName()).
                    log(Level.SEVERE, null, ex);
            Assert.fail();
          }
          long blockedOnUnhealthyChannelTime = System.currentTimeMillis() - start;
          Assert.assertTrue(
                  "Message only blocked for " + blockedOnUnhealthyChannelTime + " ms. Expected at least 4000 ms.",
                  blockedOnUnhealthyChannelTime > sleepTime); //we must have blocked longer than the unhealthy time 
        }).start();
        Thread.sleep(sleepTime); //wait couple seconds to let channel become healthy
        TriggerableUnhealthyEndpoints.healthy = true; //will unblock the HecChannel on next health poll  
        //...which will cause acknowledged to be invoked again, but then count will be 2 so test will end.
      } catch (InterruptedException ex) {
        Logger.getLogger(UnhealthyEndpointTest.class.getName()).
                log(Level.SEVERE, ex.getMessage(), ex);
      }

    }
  }

}
