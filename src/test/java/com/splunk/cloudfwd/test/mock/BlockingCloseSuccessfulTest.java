package com.splunk.cloudfwd.test.mock;

import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import com.splunk.cloudfwd.test.util.AbstractConnectionTest;
import org.junit.Test;

import java.util.Properties;
import java.util.concurrent.TimeoutException;

import static com.splunk.cloudfwd.PropertyKeys.MOCK_HTTP_CLASSNAME;
import static com.splunk.cloudfwd.PropertyKeys.MOCK_HTTP_KEY;
import static com.splunk.cloudfwd.PropertyKeys.UNRESPONSIVE_MS;

/**
 * Created by eprokop on 11/1/17.
 */
public class BlockingCloseSuccessfulTest extends AbstractConnectionTest {
    @Test
    public void testBlockingCloseSuccessful() throws TimeoutException, InterruptedException, HecConnectionTimeoutException {
        super.sendEvents(5000);
    }

    @Override
    protected Properties getProps() {
        Properties props = new Properties();
        props.setProperty(MOCK_HTTP_KEY, "true");
        props.setProperty(MOCK_HTTP_CLASSNAME,
                "com.splunk.cloudfwd.impl.sim.SimulatedHECEndpoints");
        props.setProperty(UNRESPONSIVE_MS,
                "-1");
        return props;
    }

    protected int getNumEventsToSend() {
        return 100000;
    }
}
