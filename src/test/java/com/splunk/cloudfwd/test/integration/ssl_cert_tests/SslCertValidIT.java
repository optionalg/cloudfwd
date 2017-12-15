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
import com.splunk.cloudfwd.HecHealth;
import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import com.splunk.cloudfwd.test.util.AbstractConnectionTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * This test enables SSL Verification and attempts to instantiate connection
 * to an ELB with a valid splunkcloud.com SSL certificate.  
 * 
 * @author ssergeev
 */
public class SslCertValidIT extends AbstractConnectionTest {
  
  /**
   * setUp method in the base class instantiates connection, which blocks and 
   * awaits for preflight to complete. Having at least one healthy channel 
   * confirms that SSL connection was properly set up. 
   * 
   * @throws InterruptedException
   * @throws HecConnectionTimeoutException
   */
  @Test
  public void getHealthCheckPass() throws InterruptedException, HecConnectionTimeoutException {
    List<HecHealth> healths = super.healthCheck();
    int healthyCount = (int) healths.stream().filter(HecHealth::isHealthy).count();
    String ssl_cert = this.connection.getSettings().getSSLCertContent();
    Assert.assertEquals("SSL cert expected to be empty, but got non-empty one: " + ssl_cert, ssl_cert, "");
    Assert.assertTrue("Expected to get at least one healthy channels, but got " 
            + healthyCount + ", health checks: " + healths, healthyCount > 0);
  }
  
  @Override
  protected ConnectionSettings getTestProps() {
    return new ConnectionSettings();  
  }
  
  @Override
  protected void configureProps(ConnectionSettings settings) {
    settings.setUrls("https://http-inputs-kinesis1.splunkcloud.com:443");
    settings.setToken("DB22D948-5A1D-4E73-8626-0AB3143BEE47");
    settings.enableCertValidation();
    settings.setMockHttp(false);
    // Despite we overwrite getTestProps here, ConnectionSettings will read
    // cloudfwd.properties file anyway. So we set CLOUD_SSL_CERT_CONTENT to
    // empty string explicitly.
    //FIXME: This call below should set this.cloudSslCertContent, not this.sslCertContent (fails isCloudInstance()) call
    settings.setSSLCertContent("");
  }

  @Override
  protected int getNumEventsToSend() {
    return 0;
  }

}
