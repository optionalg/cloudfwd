
import com.splunk.cloudfwd.Event;
import com.splunk.cloudfwd.HecConnectionTimeoutException;
import com.splunk.cloudfwd.PropertyKeys;
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
public class ConnectionTimeoutTest extends AbstractConnectionTest {

  @Override
  protected Properties getProps() {
    Properties props = new Properties();
    //A realistic value of BLOCKING_TIMEOUT_MS would be 1 or more MINUTES, but let's not
    //make this test run too slowly. The point is, we want to SEE the HecConnectionTimeout
    //happen repeatedly, until the message goes through
    props.put(PropertyKeys.BLOCKING_TIMEOUT_MS, "1000"); //block for 100 ms before HecConnectionTimeout
    //install an endpoint that takes 10 seconds to return an ack
    props.put(PropertyKeys.MOCK_HTTP_CLASSNAME, "com.splunk.cloudfwd.sim.errorgen.slow.SlowEndpoints");
    return props;
  }

  @Override
  protected void sendEvents() throws InterruptedException {
    System.out.println(
            "SENDING EVENTS WITH CLASS GUID: " + TEST_CLASS_INSTANCE_GUID
            + "And test method GUID " + testMethodGUID);
    int expected = getNumEventsToSend();
    for (int i = 0; i < expected; i++) {
      Event event = nextEvent(i + 1);
      System.out.println("Send event: " + event.getId() + " i=" + i);
      int connTimeoutCount = 0;
      while (true) {
        try {
          connection.send(event); 
          break;
        } catch (HecConnectionTimeoutException e) {
          if(connTimeoutCount ++ >20){ 
            Assert.fail("Too many HecConnectionTimeouts");
            return;
          }          
        }
      }
      Assert.assertTrue("Did not get expected HecConnectionTimeouts", connTimeoutCount>10);
    }
    connection.close(); //force close, since we are using a jammed up
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
  public void testHecConnectionTimeout() throws InterruptedException{
    sendEvents();
  }

}
