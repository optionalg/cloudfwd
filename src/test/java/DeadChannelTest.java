
import com.splunk.cloudfwd.PropertiesFileHelper;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

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
public class DeadChannelTest extends LoadBalancerTest {

  public static void main(String[] args) throws InterruptedException, TimeoutException {
    new DeadChannelTest().runTests();
  }

  @Override
  protected Properties getProps() {
    Properties props = new Properties();
    props.put(PropertiesFileHelper.MOCK_HTTP_KEY, "true");
    props.put(PropertiesFileHelper.MOCK_HTTP_CLASSNAME_KEY,
            "com.splunk.cloudfwd.sim.errorgen.ackslost.LossyEndpoints");
    return props;
  }

  @Override
  protected int getNumBatchesToSend() {
    return 100000;
  }
}
