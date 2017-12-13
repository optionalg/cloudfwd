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
import com.splunk.cloudfwd.error.HecConnectionTimeoutException;

import com.splunk.cloudfwd.impl.util.PropertiesFileHelper;
import com.splunk.cloudfwd.test.util.AbstractConnectionTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * This test enables SSL Verification and attempts to instantiate connection
 * to a splunk>cloud trial single instance with a splunk>cloud issued SSL cert.
 * Cloudfwd should accept these certs in cloud.splunk.com domain with a public 
 * ssl key provided in default cloudfwd.properties file.  
 * 
 * @author ssergeev
 */
public class SslCertValidCloudTrialIT extends AbstractConnectionTest {
  
  /* 
   * setUp method in the base class instantiates connection, which blocks and 
   * awaits for preflight to complete. Having at least one healthy channel 
   * confirms that SSL connection was properly set up. 
   */
  @Test
  public void getHealthCheckPass() throws InterruptedException, HecConnectionTimeoutException {
    List<HecHealth> healths = super.healthCheck();
    int healthyCount = (int) healths.stream().filter(e->e.isHealthy()).count();
    Assert.assertTrue("Expected to get at least one healthy channels, but got "
            + healthyCount + ", health checks: " + healths, healthyCount > 0);
  }
  
  @Override
  protected void configureProps(PropertiesFileHelper settings) {
    settings.setUrls("https://input-prd-p-kzgcxv8qsv24.cloud.splunk.com:8088");
    settings.setToken("19FD13FC-8C67-4E5C-8C2B-E39E6CC76152");
    settings.enableCertValidation();
    settings.setMockHttp(false);
  }
  
  @Override
  protected int getNumEventsToSend() {
    return 0;
  }

}
