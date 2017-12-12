package com.splunk.cloudfwd.test.integration;

import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.error.HecNoValidChannelsException;
import com.splunk.cloudfwd.test.util.AbstractConnectionTest;
import com.splunk.cloudfwd.test.util.BasicCallbacks;
import org.junit.Test;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Created by eprokop on 11/3/17.
 */
public class PreflightTimeoutIT extends AbstractConnectionTest {
    
    @Override
    protected int getNumEventsToSend() {
        return 1;
    }
    
    @Test
    public void unresponsiveUrlTest() throws InterruptedException { super.sendEvents(); }

    @Override
    protected Properties getProps() {
        Properties p = super.getProps();
        p.setProperty(PropertyKeys.COLLECTOR_URI, "https://kinesis4.splunkcloud.com:8088"); // URL with HEC not enabled 
        p.setProperty(PropertyKeys.MOCK_HTTP_KEY, "false");
        p.setProperty(PropertyKeys.RETRIES, "3");
        p.setProperty(PropertyKeys.MAX_TOTAL_CHANNELS, "4");
        p.setProperty(PropertyKeys.PREFLIGHT_TIMEOUT_MS, "5000");
        p.put(PropertyKeys.EVENT_BATCH_SIZE, "0");
        return p;
    }
    
    
    @Override
    protected boolean shouldSendThrowException() { return true; }
    
    @Override
    protected boolean isExpectedSendException(Exception e) {
        if (e instanceof HecNoValidChannelsException) {
            return e.getMessage().equals("No valid channels available due to possible misconfiguration."); }
        return false;
    }
    
    @Override
    protected BasicCallbacks getCallbacks() {
        return new BasicCallbacks(getNumEventsToSend()) {
            @Override
            public void await(long timeout, TimeUnit u) throws InterruptedException {
                // don't need to wait for anything since we don't get a failed callback
            }
        };
    }
}
