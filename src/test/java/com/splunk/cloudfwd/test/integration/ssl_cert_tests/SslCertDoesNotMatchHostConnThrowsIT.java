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
import com.splunk.cloudfwd.test.util.AbstractConnectionTest;
import com.splunk.cloudfwd.test.util.BasicCallbacks;
import org.junit.Test;

import javax.net.ssl.SSLPeerUnverifiedException;
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
public class SslCertDoesNotMatchHostConnThrowsIT extends AbstractConnectionTest {
  
  @Test
  /**
   * This test expects that send HecNoValidChannelsException will be thrown 
   * during the send and validates that all channels became unhealthy caused by
   * SSLPeerUnverifiedException exception. 
   */
  public void connCreationThrows() throws InterruptedException, HecConnectionTimeoutException {
      LOG.info("test: sendThrowsAndHealthContainsException");
  }
  
  @Override
  protected void configureProps(ConnectionSettings settings) {
    settings.setConntctionThrowsExceptionOnCreation(true);
    settings.setUrls("https://kinesis1-indexers-229328170.us-east-1.elb.amazonaws.com:443");
    settings.setToken("DB22D948-5A1D-4E73-8626-0AB3143BEE47");
    settings.enableCertValidation();
    settings.setMockHttp(false);
  }
  
  
      @Override
    protected int getNumEventsToSend() {
        return 0;
    }
    
    protected boolean isExpectedConnInstantiationException(Exception e) {
        return e.getCause() instanceof SSLPeerUnverifiedException;
    }
  
    /**
     * Override in test if your test wants Connection instantiation to fail
     * @return
     */
    protected boolean connectionInstantiationShouldFail() {
        return true;
    }


  @Override
  protected BasicCallbacks getCallbacks() {
    return new BasicCallbacks(getNumEventsToSend()) {
      @Override
      public void await(long timeout, TimeUnit u) throws InterruptedException {
        // don't need to wait for anything since we don't get a failed callback
      }
    };
  }
  
}
