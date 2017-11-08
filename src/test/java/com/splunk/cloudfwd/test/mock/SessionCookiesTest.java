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


public class SessionCookiesTest extends AbstractConnectionTest {

    private static final Logger LOG = LoggerFactory.getLogger(SessionCookiesTest.class.getName());

    @Test
    public void testWithSessionCookies() throws InterruptedException {
        List<String> listofChannelIds = getChannelId(this.connection);
        sendEvents();
        List<String> listofChannelsAfterCookieChanges = getChannelId(this.connection);
        for (String i : listofChannelsAfterCookieChanges) {
            if (listofChannelIds.contains(i)) {
                Assert.fail("Channel Id never changed after toggling cookies.");
            }
        }
    }

    protected Event nextEvent(int i) {
        if (i == 1 || i == getNumEventsToSend() / 2) {
            LOG.trace("Toggling cookies twice while sending message: {}", i);
            UpdateableCookieEndpoints.toggleCookie();
        }
        return super.nextEvent(i);
    }


    @Override
    protected int getNumEventsToSend() {
        return 100;
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

        return props;
    }

}