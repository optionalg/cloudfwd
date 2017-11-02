package com.splunk.cloudfwd.test.mock;

import com.splunk.cloudfwd.error.HecConnectionStateException;
import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import com.splunk.cloudfwd.test.util.AbstractConnectionTest;
import com.splunk.cloudfwd.test.util.BasicCallbacks;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;
import java.util.concurrent.TimeoutException;

import static com.splunk.cloudfwd.PropertyKeys.CHANNELS_PER_DESTINATION;
import static com.splunk.cloudfwd.PropertyKeys.MAX_TOTAL_CHANNELS;
import static com.splunk.cloudfwd.PropertyKeys.MOCK_HTTP_CLASSNAME;
import static com.splunk.cloudfwd.PropertyKeys.MOCK_HTTP_KEY;
import static com.splunk.cloudfwd.PropertyKeys.UNRESPONSIVE_MS;

/**
 * Created by eprokop on 11/1/17.
 */
public class BlockingCloseEventFailuresTest extends AbstractConnectionTest {
    @Test
    public void testBlockingCloseEventFailures() throws TimeoutException, InterruptedException, HecConnectionTimeoutException {
        super.sendEvents(5000);
        Assert.assertEquals("All events should fail since simulated endpoints don't return any acks", 
            getNumEventsToSend(), callbacks.getFailedCount());
    }
    
    protected BasicCallbacks getCallbacks() {
        return new BasicCallbacks(getNumEventsToSend()) {
            @Override
            public boolean shouldFail() {
                return true;
            }

            @Override
            protected boolean isExpectedFailureType(Exception e) {
                return e instanceof HecConnectionStateException
                    && ((HecConnectionStateException)e).getType() == HecConnectionStateException.Type.CONNECTION_CLOSED;
            }
            
        };
    }

    @Override
    protected Properties getProps() {
        Properties props = new Properties();
        props.setProperty(MOCK_HTTP_KEY, "true");
        props.setProperty(CHANNELS_PER_DESTINATION, "8");
        props.setProperty(MAX_TOTAL_CHANNELS, "8");
        props.setProperty(MOCK_HTTP_CLASSNAME,
                "com.splunk.cloudfwd.impl.sim.errorgen.ackslost.AllLossyEndpoints");
        props.setProperty(UNRESPONSIVE_MS,
            "-1");
        return props;
    }

    protected int getNumEventsToSend() {
        return 8;
    }
}
