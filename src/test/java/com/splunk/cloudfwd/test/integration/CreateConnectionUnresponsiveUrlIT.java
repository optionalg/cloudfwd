package com.splunk.cloudfwd.test.integration;

import com.splunk.cloudfwd.HecHealth;
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
public class CreateConnectionUnresponsiveUrlIT extends AbstractConnectionTest {
    @Test
    public void unresponsiveUrlTest() throws InterruptedException {
        super.sendEvents();
    }

    @Override
    protected Properties getProps() {
        Properties p = super.getProps();
        p.setProperty(PropertyKeys.COLLECTOR_URI, "https://kinesis4.splunkcloud.com:8088"); // URL with HEC not enabled 
        p.setProperty(PropertyKeys.MOCK_HTTP_KEY, "false");
        p.setProperty(PropertyKeys.RETRIES, "3");
        p.setProperty(PropertyKeys.MAX_TOTAL_CHANNELS, "4");
        // TODO: add a preflight timeout property
        return p;
    }
    
    @Override
    protected int getNumEventsToSend() {
        return 1;
    }
    
    @Override
    protected boolean shouldSendThrowException() {
        return true;
    }

    @Override
    protected boolean isExpectedSendException(Exception e) {
        if (e instanceof HecNoValidChannelsException) {
            HecNoValidChannelsException ex = (HecNoValidChannelsException) e;
            return ex.getHecHealthList().stream().allMatch((h)->!h.passedPreflight());
        }
        return false;
    }
    
    @Override
    protected BasicCallbacks getCallbacks() {
        return new BasicCallbacks(getNumEventsToSend()) {
            @Override
            public void await(long timeout, TimeUnit u) throws InterruptedException {
                // don't need to wait for anything
            }
        };
    }
}
