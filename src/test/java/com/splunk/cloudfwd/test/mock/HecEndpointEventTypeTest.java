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

import com.splunk.cloudfwd.Connection.HecEndpoint;
import com.splunk.cloudfwd.Event;
import com.splunk.cloudfwd.test.util.AbstractConnectionTest;
import com.splunk.cloudfwd.impl.util.PropertiesFileHelper;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *Note this test is excluded by default in maven pom.xml
 * @author ajayaraman
 */
public class HecEndpointEventTypeTest extends AbstractConnectionTest {
  private static final Logger LOG = LoggerFactory.getLogger(HecEndpointEventTypeTest.class.getName());

  @Override
  protected int getNumEventsToSend() {
    return 2;
  }

 
  @Override
  protected void setProps(PropertiesFileHelper settings) {
    settings.setMockHttp(true);
  }

  @Test
  public void testTextToRaw() throws Exception {
    LOG.trace("testBlobToRaw");
    this.eventType = Event.Type.TEXT;
    setEndpointType(HecEndpoint.RAW_EVENTS_ENDPOINT);
    super.sendEvents();
  }

  @Test
  public void testJsonToRaw() throws Exception {
    LOG.trace("testJsonToRaw");
    this.eventType = Event.Type.JSON;
    setEndpointType(HecEndpoint.RAW_EVENTS_ENDPOINT);
    super.sendEvents();

  }

  @Test
  public void testTextToEvent() throws Exception {
    LOG.trace("testBlobToEvent");
    this.eventType = Event.Type.TEXT;
    setEndpointType(HecEndpoint.STRUCTURED_EVENTS_ENDPOINT);
    super.sendEvents();

  }

  @Test
  public void testJsonToEvent() throws Exception {
    LOG.trace("testJsonToEvent");
    this.eventType = Event.Type.JSON;
    setEndpointType(HecEndpoint.STRUCTURED_EVENTS_ENDPOINT);
    super.sendEvents();

  }

  private void setEndpointType(HecEndpoint type) {
    super.connection.getSettings().setHecEndpointType(type);
  }

}
