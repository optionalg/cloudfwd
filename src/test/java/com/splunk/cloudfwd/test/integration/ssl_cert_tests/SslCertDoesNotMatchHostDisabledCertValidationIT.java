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

import com.splunk.cloudfwd.ConnectionSettings;
import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import com.splunk.cloudfwd.error.HecServerErrorResponseException;
import com.splunk.cloudfwd.test.util.AbstractConnectionTest;
import com.splunk.cloudfwd.test.util.BasicCallbacks;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * This test attempts to connect to ELB configured with a splunkcloud.com cert by 
 * AWS generated DNS name in amazonaws.com. Java ssl framework should fail to 
 * validate SSL connection, as provided hostname 
 * (kinesis1-indexers-229328170.us-east-1.elb.amazonaws.com) is not in the same 
 * domain as splunkcloud.com SSL certificate.
 * 
 * @author ssergeev
 */
public class SslCertDoesNotMatchHostDisabledCertValidationIT extends AbstractConnectionTest {
  
  @Test
  /**
   * This test makes sure that connection creation doesn't throw an exception if Cert Validation is disabled
   */
  public void sendEventsSuccessfully() throws InterruptedException, HecConnectionTimeoutException {
    // no-op, making sure that connection instantiation succeeded  
  }
  
  @Override
  protected void configureProps(ConnectionSettings settings) {
    settings.setUrls("https://kinesis1-indexers-229328170.us-east-1.elb.amazonaws.com:443");
    settings.setToken("DB22D948-5A1D-4E73-8626-0AB3143BEE47");
    settings.disableCertValidation();
    settings.setMockHttp(false);
  }
  
  @Override
  protected int getNumEventsToSend() {
    return 0;
  }
  
//  @Override
//  protected BasicCallbacks getCallbacks() {
//    return new BasicCallbacks(getNumEventsToSend()) {
//      @Override
//      public void await(long timeout, TimeUnit u) throws InterruptedException {
//        // don't need to wait for anything since we don't get a failed callback
//      }
//    };
//  }
  
}
