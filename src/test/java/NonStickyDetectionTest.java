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

import com.splunk.cloudfwd.IllegalHECAcknowledgementStateException;
import com.splunk.cloudfwd.PropertiesFileHelper;
import com.splunk.cloudfwd.http.EventBatch;
import com.splunk.cloudfwd.http.HttpEventCollectorEventInfo;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author ghendrey
 */
public class NonStickyDetectionTest {

  public NonStickyDetectionTest() {
  }

  @BeforeClass
  public static void setUpClass() {
  }

  @AfterClass
  public static void tearDownClass() {
  }

  @Before
  public void setUp() {
  }

  @After
  public void tearDown() {
  }

  // TODO add test methods here.
  // The methods must be annotated with annotation @Test. For example:
  //
  @Test
  public void checkNonStickyChannelDetected() throws TimeoutException, InterruptedException {
    Properties props = new Properties();
    props.put(PropertiesFileHelper.MOCK_HTTP_KEY, "true");
    props.put(PropertiesFileHelper.MOCK_HTTP_CLASSNAME_KEY,
            "com.splunk.cloudfwd.sim.errorgen.nonsticky.Endpoints");
    com.splunk.cloudfwd.Connection c = new com.splunk.cloudfwd.Connection(props);
    CountDownLatch latch = new CountDownLatch(1);
    c.setExceptionHandler((e) -> {
      Assert.
              assertTrue(e.getMessage(),
                      e instanceof IllegalHECAcknowledgementStateException);
      System.out.println("Got expected exception: " + e);
      c.close();
      latch.countDown();
    });
    int max = 1000;
    try {
      for (int i = 0; i < max; i++) {
        final EventBatch events = new EventBatch();
        events.add(new HttpEventCollectorEventInfo("info", "seqno=" + i,
                "HEC_LOGGER",
                Thread.currentThread().getName(), new HashMap(), null, null));
        System.out.println("Send batch: " + events.getId() + " i=" + i);
        c.sendBatch(events, null);
      }
    } catch (Exception e) {
      Assert.
              assertTrue(e.getMessage(),
                      e instanceof TimeoutException);
      System.out.println("Got expected timeout exception because all channels are broken (per test design): "+e.getMessage());
    }
    latch.await();
  }

}
