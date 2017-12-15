package com.splunk.cloudfwd.test.integration;

import com.splunk.cloudfwd.ConnectionSettings;
import com.splunk.cloudfwd.error.HecConnectionStateException;
import static com.splunk.cloudfwd.error.HecConnectionStateException.Type.CHANNEL_PREFLIGHT_TIMEOUT;
import com.splunk.cloudfwd.test.util.AbstractConnectionTest;
import com.splunk.cloudfwd.test.util.BasicCallbacks;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * Created by eprokop on 11/3/17.
 */
public class PreflightTimeoutIT extends AbstractConnectionTest {
    @Test
    public void unresponsiveUrlTest() throws InterruptedException {
        super.sendEvents();
    }

    @Override
    protected void configureProps(ConnectionSettings settings) {
        settings.setUrls("https://kinesis4.splunkcloud.com:8088"); // URL with HEC not enabled
        settings.setMockHttp(false);
        settings.setMaxRetries(3);
        settings.setMaxTotalChannels(4);
        settings.setPreFlightTimeoutMS(5000);
    }
    
    @Override
    protected int getNumEventsToSend() {
        return 0;
    }
    
    protected boolean isExpectedConnInstantiationException(Exception e) {
           if (e instanceof HecConnectionStateException) {
            HecConnectionStateException ex = (HecConnectionStateException) e;
            return ((HecConnectionStateException) e).getType()==CHANNEL_PREFLIGHT_TIMEOUT;
        }
        return false;
    }
  
    /**
     * Override in test if your test wants Connection instantiation to fail
     * @return
     */
    protected boolean connectionInstantiationShouldFail() {
        return true;
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
