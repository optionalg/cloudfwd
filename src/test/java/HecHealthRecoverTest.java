/*
 * Copyright 2017 Splunk, Inc..
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import com.splunk.cloudfwd.PropertiesFileHelper;
import com.splunk.cloudfwd.http.EventBatch;
import com.splunk.cloudfwd.http.HttpEventCollectorEvent;
import java.net.URL;
import java.util.List;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author meema
 */
public class HecHealthRecoverTest extends AbstractConnectionTest {

  @Before
  @Override
  public void setUp() {
    super.setUp();
    super.connection.setSendTimeout(10000);
  }
  
  @Test
  public void testRecover() throws InterruptedException {
    super.sendEvents();
  }

  @Override
  protected Properties getProps() {
    Properties props = new Properties();
    props.put(PropertiesFileHelper.MOCK_HTTP_KEY, "true");
    //simulate endpoint with unhealthy hec that recovers
    props.put(PropertiesFileHelper.MOCK_HTTP_CLASSNAME_KEY,
        "com.splunk.cloudfwd.sim.errorgen.health.RecoveringHecEndpoints");

    // use only one logging channel and one destination for this test
    props.put(PropertiesFileHelper.CHANNELS_PER_DESTINATION_KEY, "1");
    props.put(PropertiesFileHelper.COLLECTOR_URI, "https://localhost:8088");
    props.put(PropertiesFileHelper.MOCK_FORCE_URL_MAP_TO_ONE, "true");
    return props;
  }

  @Override
  protected EventBatch nextEventBatch() {
    final EventBatch events = new EventBatch(EventBatch.Endpoint.raw,
        EventBatch.Eventtype.json);
    events.add(new HttpEventCollectorEvent("info", "nothing to see here",
        "HEC_LOGGER",
        Thread.currentThread().getName(), new HashMap(), null, null));
    return events;
  }
  
  // we want to TimeOut due to pollHealth, so send a lot until we hit
  // HEALTH_POLL_NOT_OK
  @Override
    protected int getNumBatchesToSend() {
    return 1000;
  }
}
