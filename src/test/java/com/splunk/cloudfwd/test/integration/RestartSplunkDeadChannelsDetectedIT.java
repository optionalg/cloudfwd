package com.splunk.cloudfwd.test.integration;

import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.error.HecChannelDeathException;
import com.splunk.cloudfwd.test.util.BasicCallbacks;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Created by eprokop on 10/30/17.
 */
public class RestartSplunkDeadChannelsDetectedIT extends RestartSplunkChannelsDecommissionedIT {

    // Scenario: Splunk is restarted while we are sending events (dead channel detector does the work of resending on frozen channels)
    // Expected behavior: All events should still it into Splunk (with some duplicates). Splunk restarts and resets 
    // the ack count. Unacked events are resent when dead channel is detected, so we expect
    // some duplicates in this case.
    @Override
    protected Properties getProps() {
        Properties p = super.getProps();
        p.setProperty(PropertyKeys.TOKEN, createTestToken("__singleline"));

        p.setProperty(PropertyKeys.UNRESPONSIVE_MS, "30000"); // dead channel detection

        p.setProperty(PropertyKeys.CHANNEL_DECOM_MS, "100000000"); // disable regular decommissioning
        p.setProperty(PropertyKeys.ACK_TIMEOUT_MS, "100000000"); // disable ack timeouts 
        p.setProperty(PropertyKeys.BLOCKING_TIMEOUT_MS, "100000000"); // disable blocking timeouts

        p.setProperty(PropertyKeys.EVENT_BATCH_SIZE, "0"); // make all batches don't get sent before Splunk can restart
        p.setProperty(PropertyKeys.ENABLE_CHECKPOINTS, "true");
        p.setProperty(PropertyKeys.MOCK_HTTP_KEY, "false");

        return p;
    }
    

    
    @Override
    protected BasicCallbacks getCallbacks() {
        return new BasicCallbacks(getNumEventsToSend()) {
            // test may or may not warn depending if dead channel detector kicks in or not. So don't "await" a warning
            @Override
            public void await(long timeout, TimeUnit u) throws InterruptedException {
                if(shouldFail() && !this.failLatch.await(timeout, u)){
                    throw new RuntimeException("test timed out waiting on failLatch");
                }
                //only the presense of a failure (not a warning) should cause us to skip waiting for the latch that countsdown in checkpoint
                if(!shouldFail() &&  !this.latch.await(timeout, u)){
                    throw new RuntimeException("test timed out waiting on latch");
                }
            }

            @Override
            public boolean isExpectedWarningType(Exception ex) {
                return ex instanceof HecChannelDeathException;
            }
        };
    }
}
