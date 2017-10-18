package com.splunk.cloudfwd.test.mock;

import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.Event;
import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.test.util.AbstractConnectionTest;
import java.util.Properties;
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
public class UnvalidatedBytesTest extends AbstractConnectionTest {

  @Override
  protected int getNumEventsToSend() {
    return 100;
  }
  
  @Test
  public void testUnvalidatedBytesToRaw() throws InterruptedException, HecConnectionTimeoutException{
        super.connection.getSettings().setHecEndpointType(Connection.HecEndpoint.RAW_EVENTS_ENDPOINT);
    sendEvents();
  }
  
  @Test
  public void testUnvalidatedBytesToEvents() throws InterruptedException, HecConnectionTimeoutException{
    super.connection.getSettings().setHecEndpointType(Connection.HecEndpoint.STRUCTURED_EVENTS_ENDPOINT);
    sendEvents();
  }  
  
    protected Properties getProps() {
    Properties props = new Properties(); //default behavior is no "hard coded" test-specific properties
    props.put(PropertyKeys.EVENT_BATCH_SIZE,  "16000");
    //props.put(PropertyKeys.MOCK_HTTP_KEY, "false");
    super.eventType = Event.Type.UNKNOWN;
    return props;
  }
  
  
}
