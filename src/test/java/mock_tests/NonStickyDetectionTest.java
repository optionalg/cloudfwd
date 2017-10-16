package mock_tests;/*
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

import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.ConnectionSettings;
import com.splunk.cloudfwd.Event;
import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.error.HecNonStickySessionException;
import test_utils.AbstractConnectionTest;
import test_utils.BasicCallbacks;
import static com.splunk.cloudfwd.PropertyKeys.MOCK_HTTP_CLASSNAME;
import com.splunk.cloudfwd.error.HecConnectionStateException;
import java.util.Properties;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author ghendrey
 */
public class NonStickyDetectionTest extends AbstractConnectionTest {
  private static final Logger LOG = LoggerFactory.getLogger(NonStickyDetectionTest.class.getName());

  public NonStickyDetectionTest() {
  }

  @After
  @Override
  public void tearDown() {
    //must use closeNow, because close() waits for channels to empty. But do to the failure that we are
    //*trying* to induce with this test, the channels will never empty
    this.connection.closeNow();
  }

  @Override
  protected BasicCallbacks getCallbacks() {
    return new BasicCallbacks(getNumEventsToSend()) {

          protected boolean isExpectedFailureType(Exception e) {
              if(e instanceof HecNonStickySessionException){
                NonStickyDetectionTest.super.connection.closeNow(); //in this branch we don't actually resend any messages from a non-sticky channel so they will just langish until they timeout                
                return true;
              }
              return false;
          }

          public boolean shouldFail() {
              return true;
          }
      };
  }
  
  @Override
  protected boolean isExpectedSendException(Exception e) {
    if(e instanceof HecConnectionStateException){ //because we closeNow the connection as soon as we get the HecNonStickySessionException, all other sends will fail
        HecConnectionStateException ex = (HecConnectionStateException)e;
        Assert.assertTrue("Expected SEND_ON_CLOSED_CONNECTION but got " + ex, ex.getType()==HecConnectionStateException.Type.SEND_ON_CLOSED_CONNECTION);
        return true;
    }
    return false;
  }

  @Override
  protected boolean shouldSendThrowException() {
      return false;
  }  

  @Test
  public void checkNonStickyChannelDetected() throws InterruptedException  {
      super.eventType = Event.Type.TEXT;
      super.sendEvents();    
  }

  @Override
  protected Properties getProps() {
    Properties props = new Properties();
    //simulate a non-sticky endpoint
    props.setProperty(MOCK_HTTP_CLASSNAME,
            "com.splunk.cloudfwd.impl.sim.errorgen.nonsticky.NonStickEndpoints");
        props.setProperty(PropertyKeys.MAX_TOTAL_CHANNELS,
            "1");
        props.setProperty(PropertyKeys.ACK_TIMEOUT_MS,
            "10000");        
    return props;
  }
  
  @Override
  protected void configureConnection(Connection connection) {
    connection.getSettings().setEventBatchSize(0);
  }  

  @Override
  protected int getNumEventsToSend() {
    return 1000;
  }
  /*
   public static void main(String[] args) {
    new NonStickyDetectionTest().runTests();    
  }
   */

}
