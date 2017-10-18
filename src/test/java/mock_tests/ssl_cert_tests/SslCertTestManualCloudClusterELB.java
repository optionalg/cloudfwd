package mock_tests.ssl_cert_tests;/*
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
import static com.splunk.cloudfwd.PropertyKeys.*;
import com.splunk.cloudfwd.RawEvent;
import com.splunk.cloudfwd.impl.util.PropertiesFileHelper;
import test_utils.AbstractConnectionTest;
import org.junit.Test;

import java.net.UnknownHostException;
import java.util.Properties;

/**
 *
 * @author ghendrey
 */
public class SslCertTestManualCloudClusterELB extends AbstractConnectionTest {

  protected static int MAX = 10;

  public SslCertTestManualCloudClusterELB() {
  }

  @Test
  public void sendLotsOfMessages() throws InterruptedException, HecConnectionTimeoutException {
    super.sendEvents();
  }

  @Test
  public void sendLotsOfMessagesWithBuffering() throws InterruptedException, HecConnectionTimeoutException {
    connection.getSettings().setEventBatchSize(1024*16);
    super.sendEvents();
  }

  /*
  public static void main(String[] args) throws InterruptedException, TimeoutException {
    new LoadBalancerTest().runTests();
  }
   */
  @Override
  protected void setProps(PropertiesFileHelper settings) {
    try {
      settings.setUrls("https://http-inputs-kinesis1.splunkcloud.com:443"); //http is not supported protocol. Must be https
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
    settings.setToken("DB22D948-5A1D-4E73-8626-0AB3143BEE47");
    settings.setCertValidationEnabled(false);
    settings.setHttpDebugEnabled(true);
  }

  @Override
  protected int getNumEventsToSend() {
    return MAX;
  }

  @Override
  protected RawEvent getTimestampedRawEvent(int seqno) {
    return RawEvent.fromText(
            " nothing to see here, seqno: " + seqno, seqno);
  }

}
