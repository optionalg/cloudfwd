package com.splunk.cloudfwd.test.integration;

import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.test.util.BasicCallbacks;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by eprokop on 10/10/17.
 */
public class RestartSplunkChannelsDecommissionedIT extends AbstractReconciliationTest {
    protected static final Logger LOG = LoggerFactory.getLogger(RestartSplunkChannelsDecommissionedIT.class.getName());

    @Override
    protected int getNumEventsToSend() {
        return 250000;
    }

    // Scenario: Splunk is restarted while we are sending events (normal channel decommission does the work of resending on frozen channels)
    // Expected behavior: All events should still it into Splunk (with some duplicates). Splunk restarts and resets 
    // the ack count. Unacked events are resent when channel is decomissioned, so we expect
    // some duplicates in this case.
    @Test
    public void testRestartSplunk() throws InterruptedException, IOException {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        executor.schedule(()-> {
            System.out.println("restarting Splunk");
            restartSplunk();
        }, 100, TimeUnit.MILLISECONDS);
        super.sendEvents(-1);
        verifyEventCount(getNumEventsToSend());
    }

    // Checks that Splunk indexed at least as many events as we sent
    protected void verifyEventCount(int expected) throws IOException {
        String sid = createSearchJob(httpClient);
        HttpGet httpget = new HttpGet(
                "https://" + cliProperties.get("splunkHost") + ":" + cliProperties.get("mgmtPort")
                        + "/services/search/jobs/" + sid + "/results?output_mode=json");
        LOG.info("Verifying event count with request to " + httpget.getURI());
        HttpResponse getResponse = httpClient.execute(httpget);
        String getReply = parseHttpResponse(getResponse);
        int count = json.readTree(getReply).findValue("count").asInt();
        if (count < expected) {
            Assert.fail("Search only returned " + count + " events, but "
                    + expected + " events were sent or expected.");
        }
        LOG.info("Event count verified. {} events were indexed, {} events were sent or expected (duplicates are expected).", 
            count, expected);
    }

    @Override
    protected String getSearchString() {
        return "search index=" + INDEX_NAME + " | stats count";
    }

    @Override
    protected Properties getProps() {
        Properties p = super.getProps();
        p.setProperty(PropertyKeys.TOKEN, createTestToken("__singleline"));
        
        p.setProperty(PropertyKeys.CHANNEL_DECOM_MS, Long.toString(PropertyKeys.MIN_DECOM_MS)); // regular decommissioning
        
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
        return new BasicCallbacks(getNumEventsToSend());
    }
    
}
