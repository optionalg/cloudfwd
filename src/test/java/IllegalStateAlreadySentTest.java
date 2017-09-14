
import com.splunk.cloudfwd.*;

import static com.splunk.cloudfwd.PropertyKeys.MOCK_HTTP_CLASSNAME;
import static com.splunk.cloudfwd.PropertyKeys.MOCK_HTTP_KEY;
import java.util.Properties;
import org.junit.Assert;
import org.junit.Before;
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
public class IllegalStateAlreadySentTest extends AbstractConnectionTest {
  private HecConnectionStateException.Type expectedExType;

  @Override
  protected Properties getProps() {
    Properties props = new Properties();
    props.put(MOCK_HTTP_KEY, "true");
    props.put(MOCK_HTTP_CLASSNAME,
            "com.splunk.cloudfwd.sim.errorgen.slow.SlowEndpoints");
    props.put(PropertyKeys.EVENT_BATCH_SIZE, "0"); //make sure no batching
    props.put(PropertyKeys.MAX_TOTAL_CHANNELS, "1"); //so we insure we resend on same channel   
    props.put(PropertyKeys.ACK_TIMEOUT_MS, "-1");     
    props.put(PropertyKeys.ENABLE_CHECKPOINTS, "true"); //checkpoints are required for this to work      
    return props;
  }
  
  @Before
  public void setUp() {
    super.setUp();
    this.expectedExType = getExceptionType();
  }
  
  protected HecConnectionStateException.Type getExceptionType(){
    return HecConnectionStateException.Type.ALREADY_SENT;
  }

  protected void sendEvents() throws InterruptedException, HecConnectionTimeoutException {
    System.out.println(
            "SENDING EVENTS WITH CLASS GUID: " + TEST_CLASS_INSTANCE_GUID
            + "And test method GUID " + testMethodGUID);
    int expected = getNumEventsToSend();
    if (expected < 2) {
      throw new RuntimeException(
              "improperly configured test. Need at least 2 events.");
    }
    Event event = nextEvent(1);
    connection.send(event); //send a first event. The event will get stuck waiting for acknowledgement
    int exceptionCount = 0;
    for (int i = 0; i < expected - 1; i++) {
      System.out.println("Sending Duplicate Event ID");
      event = nextEvent(1); //duplicate event ID
      try {
        connection.send(event);
        Assert.fail("Succeeded in sending a message, where HecConnectionStateException was expected");
      } catch (HecConnectionStateException ex) {
        Assert.assertTrue(
                "Excpected Exception wasn't HecConnectionStateException. Was " + ex.
                getClass().getName(), ex instanceof HecConnectionStateException);
        if (ex instanceof HecConnectionStateException) {
          HecConnectionStateException e = (HecConnectionStateException) ex;
          Assert.assertEquals(
                  "HecConnectionStateException type was unexpected: " + e.getType(),
                   this.expectedExType,e.getType());
          exceptionCount++;
        }
      }
    }
    Assert.assertEquals( //all the messages except the first should have failed
            "Did not receive correct number of HecConnectionStateException",
            expected - 1, exceptionCount);
    connection.close(); //will flush 
    //note we don't need to wait for callbacks latch on this test
    if (callbacks.isFailed()) {
      Assert.fail(
              "There was a failure callback with exception class  " + callbacks.
              getException() + " and message " + callbacks.getFailMsg());
    }
  }

  /*
  protected BasicCallbacks getCallbacks() {
    return new ExpectingHecIllegalState(getNumEventsToSend());
  }
   */
  @Override
  protected int getNumEventsToSend() {
    return 1000;
  }

  @Override
  protected Event nextEvent(int seqno) {
    return super.nextEvent(1); //force duplicate event seqno by always using 1
  }

  @Test
  public void testDuplicateEvent() throws InterruptedException, HecConnectionTimeoutException {
    sendEvents();
  }
}
