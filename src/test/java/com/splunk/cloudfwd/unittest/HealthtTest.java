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
package com.splunk.cloudfwd.unittest;

import com.splunk.cloudfwd.http.AbstractHttpCallback;
import com.splunk.cloudfwd.http.HttpEventCollectorSender;
import com.splunk.cloudfwd.http.AckManager;
import com.splunk.cloudfwd.http.ChannelMetrics;
import com.splunk.cloudfwd.ConfiguredObjectFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author mesguerra
 */
public class HealthtTest {

  public HealthtTest() {
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
  public void hecSenderTest() throws InterruptedException {
	  ConfiguredObjectFactory factory = new ConfiguredObjectFactory();
	  HttpEventCollectorSender sender = factory.createSender();
	  
	  CountDownLatch latch = new CountDownLatch(1);
	  FutureCallback<HttpResponse> cb = new AbstractHttpCallback() {
		  @Override
		  public void failed(Exception ex) {
			  org.junit.Assert.fail("failed, http error");
		  }
	
		  @Override
		  public void cancelled() {
			  org.junit.Assert.fail("failed, http response was somehow cancelled");
		  }
	
		  @Override
		  protected void completed(String reply, int code) {
			  //TODO: check that we have 200
			  latch.countDown();
		  }
	  };
	  sender.pollHealth(cb);
	  latch.await(10000, TimeUnit.MILLISECONDS);
  }
  
  @Test
  public void ackManagerTest() throws InterruptedException {
	  ConfiguredObjectFactory factory = new ConfiguredObjectFactory();
	  HttpEventCollectorSender sender = factory.createSender();
	  AckManager manager = sender.getAckManager();
	  
	  manager.pollHealth();
	  
	  // wait for poll response
	  Thread.sleep(10000);;
	  ChannelMetrics cm = manager.getChannelMetrics();
	  
	  if (!cm.getChannelHealth()) {
		 org.junit.Assert.fail("fail, expecting healthy HEC in this test");
	  }
  }
  
  /*
   * TODO:
   * 1. functional - test polling started regularly
   * 2. negative tests - sender failed or received 503 and 400
   * 3. manager.pollHealth - need to tests for 400/503 or failed by http 
   */

  public static void main(String[] args) throws InterruptedException {
    HealthtTest hut = new HealthtTest();
    hut.hecSenderTest();
    hut.ackManagerTest();
  }

}
