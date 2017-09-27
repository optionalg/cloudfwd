

import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
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
 * Assumes lb.properties sets event_batch_size to 4M or something large
 * @author ghendrey
 */
public class LargeBatchTest extends AbstractPerformanceTest {

  @Override
  protected int getNumEventsToSend() {
    return 100000000;
  }
  
  
  @Test
  public void sendLots() throws TimeoutException, InterruptedException, HecConnectionTimeoutException{       
   super.sendEvents();
  }
  
}
