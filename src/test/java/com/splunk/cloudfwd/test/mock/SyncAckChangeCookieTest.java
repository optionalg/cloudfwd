package com.splunk.cloudfwd.test.mock;

import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.ConnectionSettings;
import com.splunk.cloudfwd.Event;
import com.splunk.cloudfwd.HecHealth;
import com.splunk.cloudfwd.impl.http.AcknowledgementTracker;
import com.splunk.cloudfwd.impl.sim.errorgen.cookies.UpdateableCookieEndpoints;
import com.splunk.cloudfwd.impl.util.HecHealthImpl;
import com.splunk.cloudfwd.test.util.AbstractConnectionTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;


public class SyncAckTest extends AbstractConnectionTest {

    private static final Logger LOG = LoggerFactory.getLogger(SyncAckTest.class.getName());

    @Test
    public void testWithSessionCookies() throws InterruptedException {
        sendEvents(false, false);
        long channels_with_unacked_events = connection.getHealth().stream().map(h->((HecHealthImpl)h).
                getChannel().getSender().getHecIOManager().getAcknowledgementTracker()).
                filter(a->!a.getPostedButUnackedEvents().isEmpty()).count();
        LOG.debug("SyncAckTest: channels_with_unacked_events=" + channels_with_unacked_events);
        if(channels_with_unacked_events > 0) {
            Assert.fail("Some channels had unacked events stuck in a channel!");
        }
        connection.close();
    }
    
    protected Event nextEvent(int i) {
//        if (i == 1 || i == getNumEventsToSend() / 2) {
//            LOG.debug("SyncAckTest: Toggling cookies twice while sending message: {}", i);
//            UpdateableCookieEndpoints.toggleCookie();
//        }
//        List<HecHealth> healths = connection.getHealth(); 
//        LOG.debug("SyncAckTest: channels_total=" + healths.stream().count() + ", channels_healthy=" + healths.stream().filter(h->((HecHealthImpl)h).isHealthy()).count());
        return super.nextEvent(i);
    }


    @Override
    protected int getNumEventsToSend() {
        return 10000;
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
    protected void configureProps(ConnectionSettings settings) {
        settings.setMockHttpClassname("com.splunk.cloudfwd.impl.sim.errorgen.cookies.SyncAckedEndpoint");
        settings.setMaxTotalChannels(10);
    }

}