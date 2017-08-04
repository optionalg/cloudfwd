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
import com.splunk.cloudfwd.http.HttpEventCollectorEvent;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import java.util.concurrent.TimeoutException;

import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;


/**
 *
 * @author ajayaraman
 */
public class HecEndpointEventTypeTests {

    private Map<String, String > message;
    private com.splunk.cloudfwd.Connection c;

    public HecEndpointEventTypeTests() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
        System.out.println("EXIT");
        System.exit(0);
    }

    @Before
    public void setUp() {
        Properties props = new Properties();
        props.put(PropertiesFileHelper.MOCK_HTTP_KEY, "false");
        c = new com.splunk.cloudfwd.Connection(props);
        message = new HashMap<String, String>() {
            {
                put("json", "{\"event\":\"Json\"}");
                put("blob", "event: Blob");
            }};
    }

    @After
    public void tearDown() {
        c.close();
        message.clear();
   }

    public void formatEventBatchAndSend(EventBatch.Endpoint e,
                                        EventBatch.Eventtype t, String message) throws InterruptedException, TimeoutException {
        int max = 1;
        CountDownLatch latch = new CountDownLatch(1);
        for (int i = 0; i < max; i++) {
            final EventBatch events = new EventBatch(e, t);
            events.add(new HttpEventCollectorEvent("info", message, "HEC_LOGGER",
              Thread.currentThread().getName(), new HashMap(), null, null));
            System.out.println("Send batch: " + events.getId() + " i=" + i);
            c.sendBatch(events, () -> {
                System.out.println("SUCCESS CHECKPOINT " + events.getId());
                {
                    c.close();
                    latch.countDown();
                }
            });
        }
        latch.await();
    }

    @Test
    public void testBlobToRaw() throws InterruptedException, TimeoutException {
        System.out.println("testBlobToRaw");
        try {
            formatEventBatchAndSend(EventBatch.Endpoint.raw, EventBatch.Eventtype.blob,
                    this.message.get(EventBatch.Eventtype.blob.toString()));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testJsonToRaw() throws InterruptedException, TimeoutException {
        System.out.println("testJsonToRaw");
        try {
            formatEventBatchAndSend(EventBatch.Endpoint.raw, EventBatch.Eventtype.json,
                    this.message.get(EventBatch.Eventtype.json.toString()));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testBlobToEvent() throws InterruptedException, TimeoutException {
        System.out.println("testBlobToEvent");
        try {
            formatEventBatchAndSend(EventBatch.Endpoint.event, EventBatch.Eventtype.blob,
                    this.message.get(EventBatch.Eventtype.blob.toString()));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testJsonToEvent() throws InterruptedException, TimeoutException {
        System.out.println("testJsonToEvent");
        try {
            formatEventBatchAndSend(EventBatch.Endpoint.event, EventBatch.Eventtype.json,
                    this.message.get(EventBatch.Eventtype.json.toString()));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws InterruptedException, TimeoutException {
        Result result = JUnitCore.runClasses(HecEndpointEventTypeTests.class);

        for (Failure failure : result.getFailures()) {
            System.out.println(failure.toString());
        }
        System.out.println(result.wasSuccessful());
    }
}