package mock_tests;

import com.splunk.cloudfwd.Event;
import com.splunk.cloudfwd.PropertyKeys;
import test_utils.AbstractConnectionTest;

import java.util.Properties;

import org.junit.Test;
import org.slf4j.LoggerFactory;

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
public class OncePerSecondLongevityTest extends AbstractConnectionTest {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(OncePerSecondLongevityTest.class.getName());
    
    @Test
    public void sendOncePersSecond() throws InterruptedException{
        super.sendEvents();
    }

    @Override
    protected int getNumEventsToSend() {
        return Integer.MAX_VALUE; //send forever
    }
    
    @Override
    protected Event nextEvent(int seqno) {
        try {
            Thread.sleep(1000); //wait 1 second
            return super.nextEvent(seqno);
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }
    
      @Override
  protected Properties getProps() {
    Properties props = new Properties();
    //simulate a non-sticky endpoint
    props.put(PropertyKeys.MOCK_HTTP_KEY,"false");
    props.put(PropertyKeys.EVENT_BATCH_SIZE, "0"); //send immediately
    props.put(PropertyKeys.ACK_TIMEOUT_MS, "180000"); //3 minute ack timeout
    props.put(PropertyKeys.UNRESPONSIVE_MS, "300000"); //kill dead channel when no activity for 5 minutes    
    return props;
  }
    
    
    
}
