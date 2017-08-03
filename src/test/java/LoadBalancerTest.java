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

import com.splunk.cloudfwd.PropertiesFileHelper;
import com.splunk.cloudfwd.http.EventBatch;
import com.splunk.cloudfwd.http.HttpEventCollectorEventInfo;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import java.util.concurrent.TimeoutException;

/**
 *
 * @author ghendrey
 */
public class LoadBalancerTest {

  public LoadBalancerTest() {
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

  @Test
  public void hello() throws InterruptedException, TimeoutException {
    Properties props = new Properties();
    props.put(PropertiesFileHelper.MOCK_HTTP_KEY, "true");
    com.splunk.cloudfwd.Connection c = new com.splunk.cloudfwd.Connection(props);
    int max = 1000000;
    CountDownLatch latch = new CountDownLatch(1);
    for (int i = 0; i < max; i++) {
      final EventBatch events = new EventBatch();
      events.add(new HttpEventCollectorEventInfo("info", "seqno=" + i,
              "HEC_LOGGER",
              Thread.currentThread().getName(), new HashMap(), null, null));
      System.out.println("Send batch: " + events.getId() + " i=" + i);
      c.sendBatch(events, () -> {
        System.out.println("SUCCESS CHECKPOINT " + events.getId());
        if (max == Long.parseLong(events.getId())) {
          c.close();
          latch.countDown();
        }
      });
    }
    latch.await();
    System.out.println("EXIT");
    System.exit(0);

  }

  public static void main(String[] args) throws InterruptedException, TimeoutException {
    LoadBalancerTest lbt = new LoadBalancerTest();
    lbt.hello();
  }

}
