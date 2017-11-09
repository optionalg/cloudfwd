package com.splunk.cloudfwd.test.integration.ssl_cert_tests;/*
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

import com.splunk.cloudfwd.HecHealth;
import com.splunk.cloudfwd.RawEvent;
import com.splunk.cloudfwd.error.HecConnectionStateException;
import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import com.splunk.cloudfwd.error.HecMaxRetriesException;
import com.splunk.cloudfwd.test.util.AbstractConnectionTest;
import com.splunk.cloudfwd.test.util.BasicCallbacks;
import org.junit.Test;

import java.util.List;
import java.util.Properties;

import static com.splunk.cloudfwd.PropertyKeys.*;
import static com.splunk.cloudfwd.error.HecConnectionStateException.Type.CONFIGURATION_EXCEPTION;

/**
 * This test attempts to connect to ELB configured with a splunkcloud.com cert by 
 * AWS generated DNS name in amazonaws.com. Java ssl framework should fail to 
 * validate SSL connection, as provided hostname 
 * (kinesis1-indexers-229328170.us-east-1.elb.amazonaws.com) is not in the same 
 * domain as splunkcloud.com SSL certificate. 
 * 
 * @author ssergeev
 */
public class SslCertDoesNotMatchHostIT extends AbstractConnectionTest {
  
  @Test
  /**
   * This method is just to trigger test. The interesting part happens in 
   * setUp method of the base class. 
   */
  public void testConnectionInstantiationFailure() throws InterruptedException, HecConnectionTimeoutException {
    return;
  }
  
  @Override
  protected Properties getProps() {
    Properties props = new Properties();
    props.put(COLLECTOR_URI, "https://kinesis1-indexers-229328170.us-east-1.elb.amazonaws.com:443");
    props.put(TOKEN, "DB22D948-5A1D-4E73-8626-0AB3143BEE47");
    props.put(DISABLE_CERT_VALIDATION, "false");
    props.put(MOCK_HTTP_KEY, "false");
    return props;
  }

  @Override
  protected int getNumEventsToSend() {
    return 0;
  }
  
  @Override
  protected boolean connectionInstantiationShouldFail() {
    return true;
  }
  
  @Override
  protected boolean isExpectedConnInstantiationException(Exception e){
    // Fixme this should be HecConnectionStateException
    if (e instanceof HecMaxRetriesException) {
      return true;
    }
//    LOG.info("Got ((HecConnectionStateException)e).getType(): {}, e.getMessage: {}", ((HecConnectionStateException)e).getType(), e.getMessage());
//    if (e instanceof HecConnectionStateException) {
//      return ((HecConnectionStateException)e).getType() == CONFIGURATION_EXCEPTION;
//    }
    return false;
  }
}
