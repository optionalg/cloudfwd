package mock_tests;

import com.splunk.cloudfwd.ConnectionSettings;
import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import com.splunk.cloudfwd.impl.util.PropertiesFileHelper;
import test_utils.AbstractConnectionTest;
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
public class NonBatchedVolumeTest extends AbstractConnectionTest {

  protected int numToSend = 1000000;

  @Test
  public void sendWithoutBatching() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
    connection.getSettings().setEventBatchSize(0);
    super.sendEvents();
  }


  @Override
  protected void setProps(PropertiesFileHelper settings) {
    //settings.setMockHttp(true);
  }


  @Override
  protected int getNumEventsToSend() {
    return numToSend;
  }

}
