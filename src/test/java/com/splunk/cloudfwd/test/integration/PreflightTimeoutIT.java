package com.splunk.cloudfwd.test.integration;

import com.splunk.cloudfwd.ConnectionSettings;
import com.splunk.cloudfwd.error.HecConnectionStateException;
import static com.splunk.cloudfwd.error.HecConnectionStateException.Type.CHANNEL_PREFLIGHT_TIMEOUT;

import com.splunk.cloudfwd.error.HecNoValidChannelsException;
import com.splunk.cloudfwd.error.HecServerErrorResponseException;
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
        sendEvents();
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
        return 1;
    }
    
    @Override
    protected boolean shouldSendThrowException() { return true; }
    
    @Override
    protected boolean isExpectedSendException(Exception e) { return e instanceof HecNoValidChannelsException; }
}
