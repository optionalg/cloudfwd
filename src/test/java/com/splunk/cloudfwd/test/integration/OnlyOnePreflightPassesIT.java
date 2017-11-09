package com.splunk.cloudfwd.test.integration;

import com.splunk.cloudfwd.Event;
import com.splunk.cloudfwd.PropertyKeys;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
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
    protected Properties getProps() {
        Properties p = super.getProps();
        p.put(PropertyKeys.TOKEN, createTestToken(null));
        p.setProperty(PropertyKeys.COLLECTOR_URI, "https://127.0.0.1:8088,https://kinesis4.splunkcloud.com:8088");  
        p.setProperty(PropertyKeys.MOCK_HTTP_KEY, "false");
        p.setProperty(PropertyKeys.RETRIES, "3");
        p.setProperty(PropertyKeys.MAX_TOTAL_CHANNELS, "2");
        p.setProperty(PropertyKeys.PREFLIGHT_TIMEOUT_MS, "500000000");
        return p;
    }
}
