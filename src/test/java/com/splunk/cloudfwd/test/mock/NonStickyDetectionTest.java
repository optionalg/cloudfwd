package com.splunk.cloudfwd.test.mock;/*
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
import com.splunk.cloudfwd.Event;
import com.splunk.cloudfwd.error.HecNonStickySessionException;
import com.splunk.cloudfwd.test.util.AbstractConnectionTest;
import com.splunk.cloudfwd.test.util.BasicCallbacks;
import com.splunk.cloudfwd.impl.util.PropertiesFileHelper;
import org.junit.After;
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
              return e instanceof HecNonStickySessionException;
          }

          public boolean shouldFail() {
              return true;
          }
      };
  }

  @Test
  public void checkNonStickyChannelDetected() throws InterruptedException  {
      super.eventType = Event.Type.TEXT;
      super.sendEvents();    
  }

  @Override
  protected void setProps(PropertiesFileHelper settings) {
    //simulate a non-sticky endpoint
      settings.setMockHttpClassname("com.splunk.cloudfwd.impl.sim.errorgen.nonsticky.NonStickEndpoints");
      settings.setMaxTotalChannels(1);
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
