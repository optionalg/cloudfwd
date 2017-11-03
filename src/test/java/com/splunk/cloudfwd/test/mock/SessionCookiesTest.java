package com.splunk.cloudfwd.test.mock;


import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.Event;
import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import com.splunk.cloudfwd.impl.sim.errorgen.cookies.UpdateableCookieEndpoints;
import com.splunk.cloudfwd.impl.util.HecHealthImpl;
import com.splunk.cloudfwd.impl.util.PropertiesFileHelper;
import com.splunk.cloudfwd.test.util.AbstractConnectionTest;
import com.splunk.cloudfwd.test.util.BasicCallbacks;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


public class SessionCookiesTest extends AbstractConnectionTest {

    private Exception sendException;
    private String sendExceptionMsg;

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

    @Override
    protected BasicCallbacks getCallbacks() {
        return new BasicCallbacks(getNumEventsToSend());
    }

    @Override
    protected void sendEvents() throws InterruptedException, HecConnectionTimeoutException {
        int expected = getNumEventsToSend();
        if(expected <= 0){
            return;
        }
        try {
            LOG.trace(
                    "SENDING EVENTS WITH CLASS GUID: " + TEST_CLASS_INSTANCE_GUID
                            + "And test method GUID " + testMethodGUID);
            for (int i = 0; i < expected; i++) {
                if (i == 0 || i == expected/2) {
                    LOG.trace("Toggling cookies twice while sending message: {}", i);
                    UpdateableCookieEndpoints.toggleCookie();
                }
                Event event = nextEvent(i + 1);
                LOG.trace("Send event {} i={}", event.getId(), i);
                connection.send(event);
            }
        } catch(Exception e) {
            this.sendException = e;
            this.sendExceptionMsg = e.getMessage();
            LOG.warn("In Test caught exception on Connection.send(): {} with message {}", e, e.getMessage());
        }
        checkSendExceptions();
    }

    @Override
    protected int getNumEventsToSend() {
        return 100000;
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
    protected void setProps(PropertiesFileHelper settings) {
        settings.setMockHttpClassname("com.splunk.cloudfwd.impl.sim.errorgen.cookies.UpdateableCookieEndpoints");
        settings.setMaxTotalChannels(4);
    }

    @After
    public void tearDown() {
        // We want to check if the list of channels have been replaced before terminating connection.
        // So we want to keep the close separately.
        if (callbacks.isFailed()) {
            if(null != connection){
                connection.close(); //will flush
                try {
                    this.callbacks.await(10, TimeUnit.MINUTES);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                this.callbacks.checkFailures();
                this.callbacks.checkWarnings();
            }
        }
    }
}