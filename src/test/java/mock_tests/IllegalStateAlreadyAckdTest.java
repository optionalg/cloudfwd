package mock_tests;

import com.splunk.cloudfwd.ConnectionSettings;
import com.splunk.cloudfwd.Event;
import com.splunk.cloudfwd.error.HecConnectionStateException;
import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.impl.util.PropertiesFileHelper;

import static com.splunk.cloudfwd.PropertyKeys.MOCK_HTTP_KEY;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

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
public class IllegalStateAlreadyAckdTest extends IllegalStateAlreadySentTest{
  
    @Override
    protected void setProps(PropertiesFileHelper settings) {
        settings.setMockHttp(true);
        settings.setEventBatchSize(0); //make sure no batching
        settings.setMaxTotalChannels(1); //so we insure we resend on same channel
        settings.setCheckpointEnabled(true); //checkpoints are required for this to work
  }
    protected HecConnectionStateException.Type getExceptionType(){
    return HecConnectionStateException.Type.ALREADY_ACKNOWLEDGED;
  }
    
  @Override
  protected int getNumEventsToSend() {
    return 4;
  }
  
  @Override
  protected Event nextEvent(int seqno) {
      try {
        Thread.sleep(1000); //make sure prev events ack'd
      } catch (InterruptedException ex) {
        Logger.getLogger(IllegalStateAlreadyAckdTest.class.getName()).
                log(Level.SEVERE, null, ex);
        throw new RuntimeException(ex.getMessage(), ex);
      }
    return super.nextEvent(1); //force duplicate event seqno by always using 1    
  }  
}
