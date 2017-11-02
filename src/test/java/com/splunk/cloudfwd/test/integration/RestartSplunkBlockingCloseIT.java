package com.splunk.cloudfwd.test.integration;

import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.test.util.BasicCallbacks;
import org.junit.Test;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by eprokop on 11/1/17.
 */
public class RestartSplunkBlockingCloseIT extends RestartSplunkChannelsDecommissionedIT {
    @Test
    public void testRestartSplunk() throws InterruptedException, IOException {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        executor.schedule(()-> {
            restartSplunk();
        }, 100, TimeUnit.MILLISECONDS);
        super.sendEvents(15000);
        verifyEventCount(getNumEventsToSend() - callbacks.getFailedCount());
    }
    // Scenario: Splunk is restarted while we are sending events. Caller calls the "blocking" close() method.
    // Expected behavior: Since we don't know exactly when channels will get decomissioned, we may or may not see 
    // failed callbacks. The purpose of this test is to just make sure "blocking" close() does not block or hang
    // after restarting Splunk
    @Override
    protected Properties getProps() {
        Properties p = super.getProps();
        p.setProperty(PropertyKeys.TOKEN, createTestToken("__singleline"));

        p.setProperty(PropertyKeys.CHANNEL_DECOM_MS, "30000"); //  regular decommissioning
        
        p.setProperty(PropertyKeys.UNRESPONSIVE_MS, "-1"); // disable dead channel detection
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
            // this test doesn't care whether or not we get failures or warnings since the behavior is non-deterministic.
            @Override
            public void await(long timeout, TimeUnit u) throws InterruptedException {
                if(!this.latch.await(timeout, u)){
                    throw new RuntimeException("test timed out waiting on latch");
                }
            }

            @Override
            public boolean isExpectedWarningType(Exception ex) {
                return true;
            }

            @Override
            public boolean isExpectedFailureType(Exception ex) {
                return true;
            }
        };
    }
}
