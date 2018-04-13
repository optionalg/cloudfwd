package com.splunk.cloudfwd.test.mock;

import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.ConnectionSettings;
import com.splunk.cloudfwd.Event;
import com.splunk.cloudfwd.error.HecMaxRetriesException;
import com.splunk.cloudfwd.error.HecNonStickySessionException;
import com.splunk.cloudfwd.impl.sim.errorgen.cookies.UpdateableCookieEndpoints;
import com.splunk.cloudfwd.impl.sim.errorgen.cookies.UpdateableELBCookieEndpoints;
import com.splunk.cloudfwd.impl.util.HecHealthImpl;
import com.splunk.cloudfwd.test.util.AbstractConnectionTest;
import com.splunk.cloudfwd.test.util.BasicCallbacks;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;


public class ELBStickySessionViolationTest extends AbstractConnectionTest {

    private static final Logger LOG = LoggerFactory.getLogger(ELBStickySessionViolationTest.class.getName());

    @Test
    public void testWithSessionCookies() throws InterruptedException {
        List<String> listofChannelIds = getChannelId(this.connection);
        sendEvents(false, false);
        
        List<String> listofChannelsAfterCookieChanges = getChannelId(this.connection);
        for (String i : listofChannelsAfterCookieChanges) {
            if (listofChannelIds.contains(i)) {
                Assert.fail("Channel Id never changed after toggling cookies.");
            }
        }
        connection.close();
        while(connection.getHealth().size() > 0) {
            Thread.sleep(5000);
            LOG.debug("not closed channels in connection, number_of_channels={} healths={}" , connection.getHealth().size(), connection.getHealth());
        }
        
    }

    protected Event nextEvent(int i) {
        if (i>20 && (i%10) == 0 && i < getNumEventsToSend() / 2) {
            LOG.trace("Toggling cookies from event 21-100: {}", i);
            UpdateableELBCookieEndpoints.toggleELBCookie(300);
        }
        LOG.debug("number of channels={}", connection.getHealth().size());
        return super.nextEvent(i);
    }


    @Override
    protected int getNumEventsToSend() {
        return 1000;
    }

    @Override
    public List<String> getChannelId(Connection connection) {
        ArrayList channels = new ArrayList();
        for (Object c : connection.getHealth()) {
            channels.add(((HecHealthImpl) c).getChannelId());
        }
        LOG.info("List of channel ids {}", channels);
        return channels;
    }

    @Override
    protected void configureProps(ConnectionSettings settings) {
        settings.setMockHttpClassname("com.splunk.cloudfwd.impl.sim.errorgen.cookies.UpdateableELBCookieEndpoints");
        settings.setMaxTotalChannels(1);
        settings.setMaxRetries(1);
        settings.setAckTimeoutMS(10000);
    }
    
    @Override
    protected BasicCallbacks getCallbacks() {
        return new BasicCallbacks(getNumEventsToSend()) {
            @Override
            protected boolean isExpectedFailureType(Exception e){
                LOG.debug("isExpectedFailureType: e={}", e.toString());
                return (e instanceof HecNonStickySessionException);
            }
            
        };
    }

}