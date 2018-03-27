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

import java.util.concurrent.TimeUnit;

/**
 * Cloud>Trial is issued by a private Splunk certificate authority. For 
 * security compliance we should fail it without additional configuration 
 * provided. 
 * 
 * @author ssergeev
 */
public class SslCertValidCloudTrialDisabledCertValidationDisabled extends AbstractConnectionTest {
  
  @Test
  /**
   * This test makes sure that connection creation doesn't throw an exception if Cert Validation is disabled 
   */
  public void sendEventSuccessfully() throws InterruptedException, HecConnectionTimeoutException {
    //no-op
  }
  
  @Override
  protected void configureProps(ConnectionSettings settings) {
    settings.setUrls("https://input-prd-p-kzgcxv8qsv24.cloud.splunk.com:8088");
    settings.setToken("19FD13FC-8C67-4E5C-8C2B-E39E6CC76152");
    settings.disableCertValidation();
    settings.setMockHttp(false);
    settings.setSSLCertContent("");
  }
  
  @Override
  protected int getNumEventsToSend() {
    return 0;
  }
  
}
