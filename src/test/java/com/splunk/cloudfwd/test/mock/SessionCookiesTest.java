package com.splunk.cloudfwd.test.mock;

import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.ConnectionSettings;
import com.splunk.cloudfwd.Event;
import com.splunk.cloudfwd.error.HecMaxRetriesException;
import com.splunk.cloudfwd.error.HecNonStickySessionException;
import com.splunk.cloudfwd.impl.sim.errorgen.cookies.UpdateableCookieEndpoints;
import com.splunk.cloudfwd.impl.util.HecHealthImpl;
import com.splunk.cloudfwd.test.util.AbstractConnectionTest;
import com.splunk.cloudfwd.test.util.BasicCallbacks;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;


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
        return 10000;
    }


    @Override
    protected void configureProps(ConnectionSettings settings) {
        settings.setMockHttpClassname("com.splunk.cloudfwd.impl.sim.errorgen.cookies.UpdateableCookieEndpoints");
        settings.setMaxTotalChannels(1);
    }
    
    
    @Override
    protected BasicCallbacks getCallbacks() {
        return new BasicCallbacks(getNumEventsToSend()) {
            @Override
            protected boolean isExpectedFailureType(Exception e){
                LOG.debug("isExpectedFailureType: e={}", e.toString());
                return (e instanceof HecNonStickySessionException || e instanceof HecMaxRetriesException);
            }
            
        };
    }

}