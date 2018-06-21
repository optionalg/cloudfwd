package com.splunk.cloudfwd.test.mock;

import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.ConnectionSettings;
import com.splunk.cloudfwd.Event;
import com.splunk.cloudfwd.LifecycleEvent;
import com.splunk.cloudfwd.error.HecMaxRetriesException;
import com.splunk.cloudfwd.error.HecNoValidChannelsException;
import com.splunk.cloudfwd.error.HecNonStickySessionException;
import com.splunk.cloudfwd.error.HecServerErrorResponseException;
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


public class SessionCookiesRandomTest extends AbstractConnectionTest {

    private static final Logger LOG = LoggerFactory.getLogger(SessionCookiesRandomTest.class.getName());

    @Test
    public void testWithSessionCookies() throws InterruptedException {
        List<String> listofChannelIds = getChannelId(this.connection);
        LOG.debug("testWithSessionCookies: before send listofChannelIds={}", listofChannelIds);
        sendEvents(false, false);
        List<String> listofChannelsAfterCookieChanges = getChannelId(this.connection);
        LOG.debug("testWithSessionCookies: after send listofChannelsAfterCookieChanges={}", listofChannelsAfterCookieChanges);
        for (String i : listofChannelsAfterCookieChanges) {
            if (listofChannelIds.contains(i)) {
                Assert.fail("Channel Id never changed after toggling cookies.");
            }
        }
        connection.close();
//        while(connection.getHealth().size() > 0) {
//            Thread.sleep(5000);
//            LOG.debug("not closed channels in connection, number_of_channels={} healths={}" , connection.getHealth().size(), connection.getHealth());
//        }
        
    }

    protected Event nextEvent(int i) {
        if (i>100 && (i%100) == 0 && i < getNumEventsToSend() / 2) {
            LOG.trace("Toggling cookies from event 21-100: {}", i);
            UpdateableCookieEndpoints.toggleCookie();
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
        settings.setMockHttpClassname("com.splunk.cloudfwd.impl.sim.errorgen.cookies.UpdateableCookieEndpoints");
        settings.setMaxTotalChannels(1);
        settings.setMaxRetries(1);
        settings.setMaxUnackedEventBatchPerChannel(10);
        settings.setAckTimeoutMS(10000);
        settings.setNonStickyChannelReplacementDelayMs(0);
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

//    @Override
//    protected boolean shouldSendThrowException() {return true;}
//
//    @Override
//    protected boolean isExpectedSendException(Exception e) {
//        LOG.debug("isExpectedSendException: e={}", e.toString());
//        return (e instanceof HecNoValidChannelsException);
//    }

}