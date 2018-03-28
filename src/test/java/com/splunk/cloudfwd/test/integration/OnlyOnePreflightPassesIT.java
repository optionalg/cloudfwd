package com.splunk.cloudfwd.test.integration;

import com.splunk.cloudfwd.ConnectionSettings;
import com.splunk.cloudfwd.Event;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Set;

/**
 * Created by eprokop on 11/9/17.
 */
public class OnlyOnePreflightPassesIT extends AbstractReconciliationTest {
    private static final Logger LOG = LoggerFactory.getLogger(OnlyOnePreflightPassesIT.class.
            getName());

    @Override
    protected int getNumEventsToSend() {
        return 10;
    }
    
    @Test
    public void sendToSplunk() throws InterruptedException {
        super.eventType = Event.Type.TEXT;
        super.sendEvents();
        Set<String> searchResults = getEventsFromSplunk();
        verifyResults(getSentEvents(), searchResults);
    }
    
    @Override
    protected void configureProps(ConnectionSettings settings) {
        settings.setToken(createTestToken(null));
        settings.setUrls(getTestUrl() + ",https://kinesis4.splunkcloud.com:8088");  //two endpoints. The kinesis4 endpoint exsits, but isn't HEC endpoint (it's search head)
        settings.setMockHttp(false);
        settings.setMaxRetries(3);
        settings.setMaxTotalChannels(2);
        settings.setPreFlightTimeoutMS(500000000);
    }
}
