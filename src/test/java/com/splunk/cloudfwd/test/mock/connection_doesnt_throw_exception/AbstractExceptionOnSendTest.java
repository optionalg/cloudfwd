package com.splunk.cloudfwd.test.mock.connection_doesnt_throw_exception;

import com.splunk.cloudfwd.ConnectionSettings;
import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.error.HecConnectionStateException;
import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import com.splunk.cloudfwd.error.HecNoValidChannelsException;
import com.splunk.cloudfwd.test.util.AbstractConnectionTest;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.splunk.cloudfwd.test.util.BasicCallbacks;
import org.junit.Assert;

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
public class AbstractExceptionOnSendTest extends AbstractConnectionTest{

    @Override
    protected int getNumEventsToSend() {
        return 1;
    }

    @Override
    protected boolean isExpectedSendException(Exception ex) {
      LOG.info("AbstractExceptionOnSendTest, isExpectedSendException: ex: " + ex);
      if (ex instanceof HecNoValidChannelsException) {
        return true;
      }
      return false;
    }
    
    @Override
    protected void configureProps(ConnectionSettings settings) {
      super.configureProps(settings);
      settings.setMaxTotalChannels(1);
      settings.setUrls("https://127.0.0.1:8088");
    }
    
    
    protected BasicCallbacks getCallbacks() {
      return new BasicCallbacks(getNumEventsToSend()) {
        @Override
        public void await(long timeout, TimeUnit u) throws InterruptedException {
          // don't need to wait for anything since we don't get a failed callback
        }
        
        @Override
        public void systemError(Exception ex) {
          LOG.info("AbstractExceptionOnSendTest, got systemError callabck: ex: " + ex);
          Assert.assertTrue(ex instanceof HecConnectionStateException);
        }
      };
    }
    
}
