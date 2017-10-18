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
public class RestartSplunkIT extends AbstractReconciliationTest {
    protected static final Logger LOG = LoggerFactory.getLogger(RestartSplunkIT.class.getName());

    @Override
    protected int getNumEventsToSend() {
        return 250000;
    }

    // Scenario: Splunk is restarted while we are sending events
    // Expected behavior: Since restart is relatively quick and we set timeouts high, all events should
    // still it into Splunk (with some duplicates). Splunk restarts and resets the ack count.
    // Cloudfwd notices this and resends all events that haven't yet been acked, so we expect
    // some duplicates in this case.
    @Test
    public void testRestartSplunk() throws InterruptedException, IOException {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        executor.schedule(()-> {
            System.out.println("restarting Splunk");
            restartSplunk();
        }, 100, TimeUnit.MILLISECONDS);
        super.sendEvents();
        verifyEventCount();
    }

    // Checks that Splunk indexed at least as many events as we sent
    protected void verifyEventCount() throws IOException {
        String sid = createSearchJob(httpClient);
        HttpGet httpget = new HttpGet(
                "https://" + cliProperties.get("splunkHost") + ":" + cliProperties.get("mgmtPort")
                        + "/services/search/jobs/" + sid + "/results?output_mode=json");
        LOG.info("Verifying event count with request to " + httpget.getURI() + " ...");
        HttpResponse getResponse = httpClient.execute(httpget);
        String getReply = parseHttpResponse(getResponse);
        int count = json.readTree(getReply).findValue("count").asInt();
        if (count < getNumEventsToSend()) {
            Assert.fail("Search only returned " + count + " events, but "
                    + getNumEventsToSend() + " events were sent.");
        }
        LOG.info("Event count verified.");
    }

    @Override
    protected String getSearchString() {
        return "search index=" + INDEX_NAME + " | stats count";
    }

    @Override
    protected Properties getProps() {
        Properties p = super.getProps();
        p.put(PropertyKeys.TOKEN, createTestToken("__singleline"));
        p.put(PropertyKeys.CHANNEL_DECOM_MS, "60000");
        p.put(PropertyKeys.ACK_TIMEOUT_MS, "100000000"); // we don't want any timeouts
        p.put(PropertyKeys.BLOCKING_TIMEOUT_MS, "100000000");
        p.put(PropertyKeys.EVENT_BATCH_SIZE, "0"); // make all batches don't get sent before Splunk can restart
        p.put(PropertyKeys.UNRESPONSIVE_MS, "20000");
        p.put(PropertyKeys.ENABLE_CHECKPOINTS, "true");
        return p;
    }

    @Override
    protected BasicCallbacks getCallbacks() {
        return new RestartSplunkCallbacks(getNumEventsToSend());
    }

    private class RestartSplunkCallbacks extends BasicCallbacks {
        private final Logger LOG = LoggerFactory.getLogger(RestartSplunkCallbacks.class.getName());
        public RestartSplunkCallbacks(int expected) {
            super(expected);
        }

//        @Override
//        public void systemError(Exception ex) {
//            LOG.error("SYSTEM ERROR {}",ex.getMessage());
//        }

        // this test may or may not warn, depending on if dead channel detector kicks in or not
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
            return true;
        }

//        @Override
//        public boolean shouldFail() { return true; }

//        @Override
//        public boolean isExpectedFailureType(Exception e) {
//            return e instanceof HecServerErrorResponseException;
//        }
    }
}
