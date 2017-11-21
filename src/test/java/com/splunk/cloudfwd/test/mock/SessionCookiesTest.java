package com.splunk.cloudfwd.test.mock;


import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.Event;
import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.impl.sim.errorgen.cookies.UpdateableCookieEndpoints;
import com.splunk.cloudfwd.impl.util.HecHealthImpl;
import com.splunk.cloudfwd.test.util.AbstractConnectionTest;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


public class SessionCookiesTest extends AbstractConnectionTest {

    private static final Logger LOG = LoggerFactory.getLogger(SessionCookiesTest.class.getName());

    @Test
    public void testWithSessionCookies() throws InterruptedException {
        List<String> listOfChannelIds = getChannelId(this.connection);
        sendEvents(false, false);
        List<String> listOfChannelsAfterCookieChanges = getChannelId(this.connection);
        for (String i : listOfChannelsAfterCookieChanges) {
            if (listOfChannelIds.contains(i)) {
                Assert.fail("Channel Id never changed after toggling cookies.");
            }
        }
        connection.close();
    }

    protected Event nextEvent(int i) {
        if (i == 1 || i == getNumEventsToSend() / 2.0) {
            LOG.trace("Toggling cookies twice while sending message: {}", i);
            // we want to make sure that ack from the previous event will be received with the same cookie
            try {
                TimeUnit.SECONDS.wait(1);
            } catch (Exception ex) {
                LOG.error("nextEvent got interrupted");
            }
            UpdateableCookieEndpoints.toggleCookie();
        }
        return super.nextEvent(i);
    }


    @Override
    protected int getNumEventsToSend() {
        return 5;
    }

    protected List<String> getChannelId(Connection connection) {
        ArrayList channels = new ArrayList();
        for (Object c : connection.getHealth()) {
            channels.add(((HecHealthImpl) c).getChannelId());
        }
        LOG.info("List of channel ids {}", channels);
        return channels;
    }

    @Override
    protected Properties getProps() {
        Properties props = new Properties();
        props.put(PropertyKeys.MOCK_HTTP_CLASSNAME,
                "com.splunk.cloudfwd.impl.sim.errorgen.cookies.UpdateableCookieEndpoints");
        props.put(PropertyKeys.MAX_TOTAL_CHANNELS, "1");
        props.put(PropertyKeys.EVENT_BATCH_SIZE, "0");

        return props;
    }

}