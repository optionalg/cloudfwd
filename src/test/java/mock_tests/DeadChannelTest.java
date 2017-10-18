package mock_tests;

import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.ConnectionSettings;
import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import static com.splunk.cloudfwd.PropertyKeys.MAX_TOTAL_CHANNELS;
import static com.splunk.cloudfwd.PropertyKeys.MOCK_HTTP_CLASSNAME;
import static com.splunk.cloudfwd.PropertyKeys.MOCK_HTTP_KEY;
import static com.splunk.cloudfwd.PropertyKeys.UNRESPONSIVE_MS;
import com.splunk.cloudfwd.error.HecChannelDeathException;
import com.splunk.cloudfwd.impl.util.PropertiesFileHelper;
import test_utils.AbstractConnectionTest;
import test_utils.BasicCallbacks;
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

    protected BasicCallbacks getCallbacks() {
        return new BasicCallbacks(getNumEventsToSend()) {
            @Override
            protected boolean isExpectedWarningType(Exception e) {
                return e instanceof HecChannelDeathException;
            }

            @Override
            public boolean shouldWarn() {
                return true;
            }
        };
    }

  @Override
  protected void setProps(PropertiesFileHelper settings) {
        settings.setMockHttp(true);
        settings.setMockHttpClassname("com.splunk.cloudfwd.impl.sim.errorgen.ackslost.LossyEndpoints");
        settings.setUnresponsiveMS(1000);
        settings.setMaxTotalChannels(2);
  }
  
  @Override
  protected void configureConnection(Connection connection) {
    connection.getSettings().setEventBatchSize(0);
  }
  @Override
  protected int getNumEventsToSend() {
    return 10000;
  }
  
  @Test
  public void testDeadChannel() throws TimeoutException, InterruptedException, HecConnectionTimeoutException{
    super.sendEvents();
  }
}
