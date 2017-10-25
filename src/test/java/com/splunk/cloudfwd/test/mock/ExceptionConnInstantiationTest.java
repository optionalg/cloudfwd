package com.splunk.cloudfwd.test.mock;

import com.splunk.cloudfwd.error.HecConnectionStateException;
import com.splunk.cloudfwd.test.util.AbstractConnectionTest;
import com.splunk.cloudfwd.impl.util.PropertiesFileHelper;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
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
public class ExceptionConnInstantiationTest extends AbstractConnectionTest{

    @Override
    protected int getNumEventsToSend() {
        return 0;
    }

    @Override
    protected void setProps(PropertiesFileHelper settings) {
        settings.setUrls("floort");
    }
    
    @Test
    public void doIt() throws InterruptedException{
        super.sendEvents();
    }
    

    @Override
    protected boolean isExpectedConnInstantiationException(Exception e) {
       if(e instanceof HecConnectionStateException){
           return (e.getCause() != null ) && (e.getCause() instanceof MalformedURLException);                          
       }
       return false;
    }
  
    /**
     * Override in test if your test wants Connection instantiation to fail
     * @return
     */
    @Override
    protected boolean connectionInstantiationShouldFail() {
        return true;
    }    
    
    
    
    
    
}
