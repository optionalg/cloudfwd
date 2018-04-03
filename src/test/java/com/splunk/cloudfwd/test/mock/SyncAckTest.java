package com.splunk.cloudfwd.test.mock;

import com.splunk.cloudfwd.ConnectionSettings;
import com.splunk.cloudfwd.HecHealth;
import com.splunk.cloudfwd.impl.util.HecHealthImpl;
import com.splunk.cloudfwd.test.util.AbstractConnectionTest;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Future;


public class SyncAckTest extends AbstractConnectionTest {

    private static final Logger LOG = LoggerFactory.getLogger(SyncAckTest.class.getName());
    
    
    /**
     * Send events to an endpoint replying with synchronous acknowledgemen header, which should immediately 
     * acknowledge events. We validate that all events get success callback, no unacked events are in ack tracker 
     * and ack tracker not started. 
     * @throws InterruptedException
     */
    @Test
    public void testSyncAck() throws InterruptedException {
        sendEvents(false, false); //do not close connection till we validate state at the end of the test
        verifySyncAckConditions();
        connection.close(); //close connection manually, as we didn't do it as part of sendEvents call
    }
    
    public void verifySyncAckConditions() {
        //count channels with unacked events
        long channels_with_unacked_events = connection.getHealth().stream().map(h->((HecHealthImpl)h).
                getChannel().getSender().getHecIOManager().getAcknowledgementTracker()).
                filter(a->!a.getPostedButUnackedEvents().isEmpty()).count();
        LOG.debug("testSyncAck: channels_with_unacked_events=" + channels_with_unacked_events);
        if(channels_with_unacked_events > 0) {
            Assert.fail("Some channels had unacked events stuck in a channel!");
        }
    
        for (HecHealth h: connection.getHealth()) {
            Future ackPollTask = ((HecHealthImpl)h).getChannel().getSender().getHecIOManager().getAckPollTask();
            LOG.debug("testSyncAck: HecHealth="+ h+", ackPollTask=" + ackPollTask);
        }
    
        //Ack tracker task should be triggered only for asynchronous mode, so we expect it to be null for all channels 
        long channels_with_ack_polling_task = connection.getHealth().stream().map(h->((HecHealthImpl)h).
                getChannel().getSender().getHecIOManager().getAckPollTask()).
                filter(a->!(a == null)).count();
        LOG.debug("testSyncAck: channels_with_ack_polling_task=" + channels_with_ack_polling_task);
        if(channels_with_ack_polling_task > 0) {
            Assert.fail("Some channels had started ack polling tasks!");
        }
    
        // just a sanity check, we expect channels not to be closed at this point
        long openChannels = connection.getHealth().stream().count();
        if( openChannels != connection.getSettings().getChannelsPerDestination() ) {
            Assert.fail("Expected connection to have exactly channels_per_destination=" +connection.getSettings().getChannelsPerDestination() + " open, but got " + openChannels +" open channels ");
        }
    }

    @Override
    protected int getNumEventsToSend() { return 10000; }

    @Override
    protected void configureProps(ConnectionSettings settings) {
        settings.setMockHttpClassname("com.splunk.cloudfwd.impl.sim.errorgen.cookies.SyncAckedEndpoint");
        settings.setMaxTotalChannels(4);
    }

}