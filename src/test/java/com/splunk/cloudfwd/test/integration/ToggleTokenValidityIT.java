package com.splunk.cloudfwd.test.integration;

import com.splunk.cloudfwd.*;
import com.splunk.cloudfwd.error.HecNoValidChannelsException;
import com.splunk.cloudfwd.error.HecServerErrorResponseException;
import com.splunk.cloudfwd.impl.util.PropertiesFileHelper;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.splunk.cloudfwd.test.util.BasicCallbacks;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.After;

/**
 * Created by eprokop on 10/5/17.
 */
public class ToggleTokenValidityIT extends AbstractReconciliationTest {
    protected static final Logger LOG = LoggerFactory.getLogger(ToggleTokenValidityIT.class.getName());
    private String assertionFailure = null;
    private CountDownLatch tokenRestoredLatch = new CountDownLatch(1);
    private long sendExceptionTimeout = 30000; // 30 sec
    
    
    @After
    @Override
    public void tearDown(){
        super.tearDown();
        connection.closeNow(); //channels from before
    }

    // Scenario: 1) Connection is created with valid token and start sending events 2) Token is deleted on server 3) New token is created on server 4) Connection settings updated with new token
    // Behavior: Throw an exception on "send" when token is deleted and becomes invalid. After token is updated, all events should make it into Splunk.
    @Test
    public void toggleTokenValidity() throws InterruptedException {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        executor.schedule(()->{
            LOG.info("deleting token on server");
            deleteTokenOnServer();
            LOG.info("waiting for health poll to become unhealthy due to deleted token");
            try {
                Thread.sleep(connection.getSettings().getHealthPollMS()*5); // give health poll enough time to update health
                checkHealthStatusInvalidToken();
                LOG.info("restoring token");
                restoreToken();     
            } catch (Exception e) {
                this.assertionFailure = e.getMessage();
                return;
            }

        }, 3000, TimeUnit.MILLISECONDS);

        HecNoValidChannelsException e = null;
        try {
            long start = System.currentTimeMillis();
            int i = -1000; //Start these first events at negative number. It's a little hack to avoid duplicate ack's when super.sendEvents kicks in
            while (System.currentTimeMillis()-start < sendExceptionTimeout) {
                LOG.info("sending event...");
                connection.sendBatch(
                    Events.createBatch().add(RawEvent.fromText("foo", i++)));
                sleep(250); // don't overwhelm Splunk instance
            }
            Assert.fail("Test timed out waiting for sendBatch to throw an exception.");
        } catch (HecNoValidChannelsException ex) {
            LOG.info("caught expected HecNoValidChannelsException");
            e = ex;
        }
        Assert.assertNotNull("Should receive exception on send.", e);
        LOG.info("waiting for token to be restored on server");
        tokenRestoredLatch.await();
        while(!isTokenRestorationPickedUpByChannelHealthPolling()){
            LOG.info("waiting for health poll to pickup token restoration");
            Thread.sleep(500);
        }        
        LOG.info("Token restored, sending more events...");
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
    protected void setProps(PropertiesFileHelper settings) {
        super.setProps(settings);
        settings.setToken(createTestToken("__singleline"));
        // we don't want to hit any ack timeouts because it's easier to make our callbacks not expect them
        settings.setAckTimeoutMS(sendExceptionTimeout + PropertyKeys.DEFAULT_ACK_TIMEOUT_MS);
        settings.setEventBatchSize(0);  //batching MUST be disabled for this test. If not, they way that we send some events "outside" the test framework (not using sendEvents) causes acknowledged not to countdown the latch
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

    private void restoreToken() throws InterruptedException {
        LOG.info("Restoring token on server...");
        // normally if this fails it will causes the test to fail via an assert,
        // but it won't in this case since it's not being called in the main thread so we need to check
        connection.getSettings().setToken(createTestToken("__singleline"));

        if (getTokenValue() == null) {
            this.assertionFailure = "Failed to create token.";
        }
        
        LOG.info("Connection object updated with new token.");
        
        Thread.sleep(4000); //wait for channel healths to refresh
        tokenRestoredLatch.countDown();
    }

    private void checkHealthStatusInvalidToken() {
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
    
    private boolean isTokenRestorationPickedUpByChannelHealthPolling() {
        List<HecHealth> channelHealths = connection.getHealth();
        boolean allHealthy = true;
        for (HecHealth h : channelHealths) {
            allHealthy &= (h.isHealthy() & !h.isMisconfigured() && h.getStatus().getType() == LifecycleEvent.Type.HEALTH_POLL_OK);
        }
        return allHealthy;
    }

    @Override
    protected BasicCallbacks getCallbacks() {
        return new ToggleTokenCallbacks(getNumEventsToSend());
    }

    private class ToggleTokenCallbacks extends BasicCallbacks {
        private final Logger LOG = LoggerFactory.getLogger(ToggleTokenCallbacks.class.getName());
        public ToggleTokenCallbacks(int expected) {
            super(expected);
        }

        @Override
            public void systemError(Exception ex) {
                // we are not expecting any failures after the token is restored
                if (tokenRestoredLatch.getCount() == 0) {
                    super.systemError(ex);
                } else  if(!isResponseFromTokenThatIsNoLongerValid((HecServerErrorResponseException) ex)){
                    super.systemError(ex); //event post on bad token will yield system error
                }

                
            }

            @Override
            public void systemWarning(Exception ex) {
                if (tokenRestoredLatch.getCount() == 0) {
                    if (ex instanceof HecServerErrorResponseException
                        && isResponseFromTokenThatIsNoLongerValid((HecServerErrorResponseException)ex)) {
                        // even after token is restored on server, cloudfwd will continue to poll for acks on the old channels with the invalid tokens, so we will ignore those warnings
                        return;
                    }
                    super.systemWarning(ex);
                } else {
                    LOG.warn("SYSTEM WARNING {}", ex.getMessage());
                }
            }
            
            boolean isResponseFromTokenThatIsNoLongerValid(HecServerErrorResponseException ex){
                return ex.getHecErrorText().toLowerCase().contains("invalid token"); 
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
