
import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.util.PropertiesFileHelper;
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
public class NonBatchedVolumeTest extends AbstractConnectionTest{
  
  protected int numToSend = 1000000;

  @Test
  public void sendWithoutBatching() throws InterruptedException, TimeoutException {
    connection.setCharBufferSize(0);
    super.sendEvents();
  }


  @Override
  protected Properties getProps() {
    Properties props = new Properties();
    props.put(PropertiesFileHelper.MOCK_HTTP_KEY, "true");
    return props;
  }


  @Override
  protected int getNumEventsToSend() {
    return numToSend;
  }

}
