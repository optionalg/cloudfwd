package com.splunk.cloudfwd.test.mock.connection_doesnt_throw_exception;

import com.splunk.cloudfwd.ConnectionCallbacks;
import com.splunk.cloudfwd.ConnectionSettings;
import com.splunk.cloudfwd.Connections;
import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import com.splunk.cloudfwd.error.HecMaxRetriesException;
import com.splunk.cloudfwd.error.HecNoValidChannelsException;
import com.splunk.cloudfwd.test.util.AbstractConnectionTest;
import com.splunk.cloudfwd.test.util.BasicCallbacks;
import org.junit.Assert;
import org.junit.Test;

import java.net.ConnectException;
import java.util.ArrayList;
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
    protected ConnectionSettings getTestProps() {
        ConnectionSettings settings = super.getTestProps();
        settings.setConntctionThrowsExceptionOnCreation(false);
        settings.setMockHttpClassname("com.splunk.cloudfwd.impl.sim.errorgen.indexer.DownIndexerEndpoints");
        settings.setMaxTotalChannels(4);
        settings.setBlockingTimeoutMS(10000);
        settings.setHealthPollMS(1000);
        settings.setAckTimeoutMS(60000);
        settings.setUnresponsiveMS(-1);
        return settings;
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
