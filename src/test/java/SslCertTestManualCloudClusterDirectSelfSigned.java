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

import com.splunk.cloudfwd.Event;
import com.splunk.cloudfwd.util.PropertiesFileHelper;
import com.splunk.cloudfwd.RawEvent;
import org.junit.Test;

import java.util.Properties;
import java.util.concurrent.TimeoutException;

/**
 *
 * @author ghendrey
 */
public class SslCertTestManualCloudClusterDirectSelfSigned extends AbstractConnectionTest {

  protected static int MAX = 10;

  public SslCertTestManualCloudClusterDirectSelfSigned() {
  }

  @Test
  public void sendLotsOfMessages() throws InterruptedException, TimeoutException {
    super.sendEvents();
  }

  @Test
  public void sendLotsOfMessagesWithBuffering() throws InterruptedException, TimeoutException {
    connection.setCharBufferSize(1024*16);
    super.sendEvents();
  }

  /*
  public static void main(String[] args) throws InterruptedException, TimeoutException {
    new LoadBalancerTest().runTests();
  }
   */
  @Override
  protected Properties getProps() {
    Properties props = new Properties();
    //props.put(PropertiesFileHelper.MOCK_HTTP_KEY, "false");
    props.put(PropertiesFileHelper.COLLECTOR_URI, "https://inputs1.pr16-kimono.splunkcloud.com:8088");
    props.put(PropertiesFileHelper.TOKEN_KEY, "6AFC4B90-5974-478C-9EC6-5F586CDD2F46");
    props.put(PropertiesFileHelper.DISABLE_CERT_VALIDATION_KEY, "true");
    return props;
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
