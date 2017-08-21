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
import com.splunk.cloudfwd.PropertiesFileHelper;
import com.splunk.cloudfwd.RawEvent;
import java.util.Properties;
import org.junit.Test;
import java.util.concurrent.TimeoutException;

/**
 *
 * @author ghendrey
 */
public class LoadBalancerTest extends AbstractConnectionTest {

  protected static int MAX = 1000000;

  public LoadBalancerTest() {
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
    props.put(PropertiesFileHelper.MOCK_HTTP_KEY, "true");
    return props;
  }

  @Override
  protected Event nextEvent(int seqno) {
    return RawEvent.fromText("nothing to see here.", seqno);
  }

  @Override
  protected int getNumEventsToSend() {
    return MAX;
  }

}
