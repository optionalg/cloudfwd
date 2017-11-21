package com.splunk.cloudfwd.test.mock;

import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.error.HecConnectionStateException;
import com.splunk.cloudfwd.error.HecNoValidChannelsException;
import com.splunk.cloudfwd.test.util.AbstractConnectionTest;
import java.net.MalformedURLException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.splunk.cloudfwd.test.util.BasicCallbacks;
import org.junit.Test;

import static com.splunk.cloudfwd.PropertyKeys.MOCK_HTTP_KEY;

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
public class ExceptionConnInstantiationTest extends AbstractConnectionTest{

    @Override
    protected int getNumEventsToSend() {
        return 1;
    }

    @Override
    protected Properties getProps() {
       Properties props = new Properties();
       props.put(PropertyKeys.COLLECTOR_URI, "floort");
       props.put(MOCK_HTTP_KEY, "false");
       return props;
    }
    
//    @Test
//    public void doIt() throws InterruptedException{
//        super.sendEvents();
//    }
    
    @Override
    protected BasicCallbacks getCallbacks() {
      return new BasicCallbacks(getNumEventsToSend()) {
        @Override
        public void await(long timeout, TimeUnit u) throws InterruptedException {
          // don't need to wait for anything since we don't get a failed callback
        }
        
        @Override
        public void systemError(Exception ex) {
        }
      };
    }
    
}
