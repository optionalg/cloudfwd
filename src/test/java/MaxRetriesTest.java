
import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import com.splunk.cloudfwd.error.HecMaxRetriesException;
import com.splunk.cloudfwd.PropertyKeys;
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
public class MaxRetriesTest extends AbstractConnectionTest {

  private static final Logger LOG = LoggerFactory.getLogger(MaxRetriesTest.class.getName());

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
    props.put(PropertyKeys.RETRIES, "2");
    props.put(PropertyKeys.UNRESPONSIVE_MS, "250"); //for this test, lets QUICKLY determine the channel is dead
    return props;
  }
  

  @Override
  protected void sendEvents() throws InterruptedException {
    if (getNumEventsToSend() > 1) {
      throw new RuntimeException(
              "This test uses close(), not closeNow(), so don't jam it up with more than one Batch to test on "
              + "a jammed up channel. It will take too long to be practical.");
    }
    LOG.trace(
            "SENDING EVENTS WITH CLASS GUID: " + TEST_CLASS_INSTANCE_GUID
            + "And test method GUID " + testMethodGUID);
    try {
      connection.send(nextEvent(1));
    } catch (HecConnectionTimeoutException ex) {
      Assert.fail(
              "The first message should send - but we got an HecConnectionTimeoutException");
    }
    connection.close();
    this.callbacks.await(1, TimeUnit.MINUTES);
    //we set this test up to detect too many retries by DeadChannelDetector
    Assert.assertTrue("Did not get expected HecMaxRetrriesException FAILURE",
            super.callbacks.failed);
    Assert.assertTrue("Did not get expected HecMaxRetrriesException FAILURE",
            super.callbacks.getException() instanceof HecMaxRetriesException);
  }

  @Override
  protected int getNumEventsToSend() {
    return 1;
  }

  @Test
  public void testMaxRetries() throws InterruptedException {
    sendEvents();
  }
  
  @Override
  public BasicCallbacks getCallbacks(){
    return new BasicCallbacks(0){
       @Override
       protected boolean isExpectedFailureType(Exception e){
         return e instanceof HecMaxRetriesException;
       }    
       
       @Override
         public boolean shouldFail(){
            return true;
          }
    };
  }

}
