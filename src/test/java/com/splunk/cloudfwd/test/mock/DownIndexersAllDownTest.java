package com.splunk.cloudfwd.test.mock;

import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.error.HecNoValidChannelsException;
import com.splunk.cloudfwd.test.util.AbstractConnectionTest;
import org.junit.Test;

import java.util.Properties;

/**
 * Created by mhora on 10/4/17.
 * 
 * This test should attempt to send events to a down indexer endpopoint and send should fail
 * with HecNoValidChannelsException exception. 
 */
public class DownIndexersAllDownTest extends AbstractConnectionTest {
    /**
     * Send an event and expect send to fail
     * @throws InterruptedException
     */
    @Test
    public void sendToDownIndexers() throws InterruptedException {
        super.sendEvents();
    }

    protected int getNumEventsToSend() {
        return 1;
    }

    @Override
    protected Properties getProps() {
        Properties props = new Properties();
        props.put(PropertyKeys.MOCK_HTTP_CLASSNAME,
            "com.splunk.cloudfwd.impl.sim.errorgen.indexer.DownIndexerEndpoints");
        props.put(PropertyKeys.MAX_TOTAL_CHANNELS, "4");
        props.put(PropertyKeys.BLOCKING_TIMEOUT_MS, "10000");
        props.put(PropertyKeys.HEALTH_POLL_MS, "1000");
        props.put(PropertyKeys.ACK_TIMEOUT_MS, "60000");
        props.put(PropertyKeys.UNRESPONSIVE_MS, "-1"); //no dead channel detection

        return props;
    }

    @Override
    protected boolean isExpectedSendException(Exception e) {
        return (e instanceof HecNoValidChannelsException && 
                e.getMessage().equals("No valid channels available due to possible misconfiguration."));
            
    }

    @Override
    protected boolean shouldSendThrowException() {
        return true;
    }

}
