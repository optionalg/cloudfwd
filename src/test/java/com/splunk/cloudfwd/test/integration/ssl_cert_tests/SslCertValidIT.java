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
import static com.splunk.cloudfwd.PropertyKeys.*;
import com.splunk.cloudfwd.test.util.AbstractConnectionTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Properties;

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
  protected Properties getTestProps() {
    return new Properties();
  }
  
  @Override
  protected Properties getProps() {
    Properties props = new Properties();
    props.put(COLLECTOR_URI, "https://http-inputs-kinesis1.splunkcloud.com:443");
    props.put(TOKEN, "DB22D948-5A1D-4E73-8626-0AB3143BEE47");
    props.put(DISABLE_CERT_VALIDATION, "false");
    props.put(MOCK_HTTP_KEY, "false");
    // Despite we overwrite getTestProps here, ConnectionSettings will read
    // cloudfwd.properties file anyway. So we set CLOUD_SSL_CERT_CONTENT to
    // empty string explicitly.
    props.put(CLOUD_SSL_CERT_CONTENT, "");
    return props;
  }

  @Override
  protected int getNumEventsToSend() {
    return 0;
  }

}
