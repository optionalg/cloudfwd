
import com.splunk.cloudfwd.HecConnectionTimeoutException;
import static com.splunk.cloudfwd.PropertyKeys.MAX_TOTAL_CHANNELS;
import static com.splunk.cloudfwd.PropertyKeys.MOCK_HTTP_CLASSNAME;
import static com.splunk.cloudfwd.PropertyKeys.UNRESPONSIVE_MS;

import com.splunk.cloudfwd.IConnection;

import java.util.Properties;
import java.util.concurrent.TimeoutException;
import org.junit.Test;

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
/**
 *
 * @author ghendrey
 */
public class DeadChannelTest extends AbstractConnectionTest {
/*
  public static void main(String[] args) throws InterruptedException, TimeoutException {
    new DeadChannelTest().runTests();
  }
*/
  @Override
  protected Properties getProps() {
    Properties props = new Properties();
    //props.put(PropertiesFileHelper.MOCK_HTTP_KEY, "true");
    props.put(MOCK_HTTP_CLASSNAME,
            "com.splunk.cloudfwd.sim.errorgen.ackslost.LossyEndpoints");
    props.put(UNRESPONSIVE_MS,
            "5000"); //set dead channel detector to detect at 5 second    
        props.put(MAX_TOTAL_CHANNELS,
            "2");
    return props;
  }
  @Override
  protected void configureConnection(IConnection connection) {
    connection.setEventBatchSize(0);
  }
  @Override
  protected int getNumEventsToSend() {
    return 10;
  }
  
  @Test
  public void testDeadChannel() throws TimeoutException, InterruptedException, HecConnectionTimeoutException{
    super.sendEvents();
  }
}
