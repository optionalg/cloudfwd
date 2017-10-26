package com.splunk.cloudfwd.test.mock;


import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.test.util.AbstractConnectionTest;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class SessionCookiesTest extends AbstractConnectionTest {

    private static final Logger LOG = LoggerFactory.getLogger(SessionCookiesTest.class.getName());


    @Test
    public void testWithSessionCookies() throws InterruptedException {
        super.sendEvents();
    }

    @Override
    protected int getNumEventsToSend() {
        return 1;
    }

    @Override
    protected Properties getProps() {
        Properties props = new Properties();
        props.put(PropertyKeys.MOCK_HTTP_CLASSNAME,
                "com.splunk.cloudfwd.impl.sim.errorgen.cookies.SimHecEndpointsWithCookies");
        props.put(PropertyKeys.MAX_TOTAL_CHANNELS, "4");
        props.put(PropertyKeys.BLOCKING_TIMEOUT_MS, "10000");
        props.put(PropertyKeys.HEALTH_POLL_MS, "1000");
        props.put(PropertyKeys.ACK_TIMEOUT_MS, "60000");
        props.put(PropertyKeys.UNRESPONSIVE_MS, "-1"); //no dead channel detection
        return props;
    }

    @After
    @Override
    public void tearDown() {
        //must use closeNow, because close() waits for channels to empty. But do to the failure that we are
        //*trying* to induce with this test, the channels will never empty
        this.connection.closeNow();
    }
}