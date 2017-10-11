package integration_tests;

import com.splunk.cloudfwd.*;
import com.splunk.cloudfwd.error.HecNoValidChannelsException;
import com.splunk.cloudfwd.error.HecServerErrorResponseException;
import com.splunk.cloudfwd.impl.http.httpascync.HttpCallbacksAckPoll;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import test_utils.BasicCallbacks;

import java.net.UnknownHostException;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by eprokop on 10/5/17.
 */
public class ToggleTokenValidityIT extends AbstractReconciliationTest {
    protected static final Logger LOG = LoggerFactory.getLogger(ToggleTokenValidityIT.class.getName());
    private String assertionFailure = null;
    private CountDownLatch tokenRestoredLatch = new CountDownLatch(1);
    private long sendExceptionTimeout = 30000; // 30 sec

    // Scenario: 1) Connection is created with valid token and start sending events 2) Token is deleted on server 3) New token is created on server 4) Connection settings updated with new token
    // Behavior: Throw an exception on "send" when token is deleted and becomes invalid. After token is updated, all events should make it into Splunk.
    @Test
    public void toggleTokenValidity() throws InterruptedException {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        executor.schedule(()->{
            deleteTokenOnServer();
            try {
                Thread.sleep(connection.getSettings().getHealthPollMS()*5); // give health poll enough time to update health
            } catch (InterruptedException e) {
                this.assertionFailure = e.getMessage();
                return;
            }
            checkHealth();
            restoreToken();
        }, 3000, TimeUnit.MILLISECONDS);

        HecNoValidChannelsException e = null;
        try {
            long start = System.currentTimeMillis();
            int i = 0;
            while (System.currentTimeMillis()-start < sendExceptionTimeout) {
                connection.sendBatch(
                    Events.createBatch().add(RawEvent.fromText("foo", i++)));
                sleep(250); // don't overwhelm Splunk instance
            }
            Assert.fail("Test timed out waiting for sendBatch to throw an exception.");
        } catch (HecNoValidChannelsException ex) {
            e = ex;
        }
        Assert.assertNotNull("Should receive exception on send.", e);

        tokenRestoredLatch.await();
        super.sendEvents();
        Set<String> searchResults = getEventsFromSplunk();
        verifyResults(getSentEvents(), searchResults);

        if (assertionFailure != null) {
            Assert.fail(assertionFailure);
        }
    }


    @Override
    protected int getNumEventsToSend() {
        return 10;
    }

    @Override
    protected Properties getProps() {
        Properties p = super.getProps();
        p.put(PropertyKeys.TOKEN, createTestToken("__singleline"));
        // we don't want to hit any ack timeouts because it's easier to make our callbacks not expect them
        p.put(PropertyKeys.ACK_TIMEOUT_MS, Long.toString(sendExceptionTimeout)
            + PropertyKeys.DEFAULT_ACK_TIMEOUT_MS);
        return p;
    }

    private void deleteTokenOnServer() {
        // normally if this fails it will causes the test to fail via an assert,
        // but it won't in this case since it's not being called in the main thread so we need to check
        LOG.info("Deleting token on server...");
        deleteTestToken();
        LOG.info("Token deleted.");
        if (getTokenValue() != null) {
            this.assertionFailure = "Failed to delete token.";
        }
    }

    private void restoreToken() {
        Properties p = new Properties();
        LOG.info("Restoring token on server...");
        p.put(PropertyKeys.TOKEN, createTestToken("__singleline"));
        LOG.info("Token restored.");
        // normally if this fails it will causes the test to fail via an assert,
        // but it won't in this case since it's not being called in the main thread so we need to check
        if (getTokenValue() == null) {
            this.assertionFailure = "Failed to create token.";
        }
        try {
            connection.getSettings().setProperties(p);
            LOG.info("Connection object updated with new token.");
        } catch (UnknownHostException e) {
            this.assertionFailure = e.getMessage(); // should never happen in this test
        }
        tokenRestoredLatch.countDown();
    }

    private void checkHealth() {
        try {
            List<HecHealth> channelHealths = connection.getHealth();
            Assert.assertTrue(channelHealths.size() > 0);
            for (HecHealth h : channelHealths) {
                Assert.assertTrue(!h.isHealthy());
                Assert.assertTrue(h.isMisconfigured());
                Assert.assertTrue(h.getStatus().getType() == LifecycleEvent.Type.INVALID_TOKEN);
                Assert.assertTrue(h.getStatusException() instanceof HecServerErrorResponseException);
            }
        } catch(AssertionError e) {
            this.assertionFailure = e.getMessage();
        }
    }

    @Override
    protected BasicCallbacks getCallbacks() {
        return new ToggleTokenCallbacks(getNumEventsToSend());
    }

    private class ToggleTokenCallbacks extends BasicCallbacks {
        private final Logger LOG = LoggerFactory.getLogger(BasicCallbacks.class.getName());
        public ToggleTokenCallbacks(int expected) {
            super(expected);
        }

        @Override
            public void systemError(Exception ex) {
                // we are not expecting any failures after the token is restored
                if (tokenRestoredLatch.getCount() == 0) {
                    super.systemError(ex);
                } else {
                    LOG.error("SYSTEM ERROR {}",ex.getMessage());
                }
            }

            @Override
            public void systemWarning(Exception ex) {
                if (tokenRestoredLatch.getCount() == 0) {
                    if (ex instanceof HecServerErrorResponseException
                        && ((HecServerErrorResponseException)ex).getContext().equals(HttpCallbacksAckPoll.Name)) {
                        // even after token is restored on server, cloudfwd will continue to poll for acks on the old channels with the invalid tokens, so we will ignore those warnings
                        LOG.warn("SYSTEM WARNING {}", ex.getMessage());
                        return;
                    }
                    super.systemWarning(ex);
                } else {
                    LOG.warn("SYSTEM WARNING {}", ex.getMessage());
                }
            }

            @Override
            public void failed(EventBatch events, Exception ex) {
                if (tokenRestoredLatch.getCount() == 0) {
                    super.failed(events, ex);
                } else {
                    LOG.error("EventBatch failed to send. Exception message: " + ex.
                        getMessage());
                }
            }
        }
}
