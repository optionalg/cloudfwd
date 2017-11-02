package com.splunk.cloudfwd.test.mock;

import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.error.HecConnectionStateException;
import com.splunk.cloudfwd.test.util.AbstractConnectionTest;
import com.splunk.cloudfwd.test.util.BasicCallbacks;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by eprokop on 11/1/17.
 * 
 * The purpose of these tests is to test the different "close" methods while we are 
 * sending events concurrently just to make sure the test doesn't hang or throw 
 * unexpected exceptions. Test does NOT care whether or not we see failures or warnings.
 */
public class CloseTest extends AbstractConnectionTest {
    Exception ex;
    
    @Test
    public void blockingCloseTest() throws InterruptedException {
        scheduleClose(5000); //5 seconds
    }

    @Test
    public void closeTest() throws InterruptedException {
        scheduleClose(0);
    }

    @Test
    public void closeNowTest() throws InterruptedException {
        scheduleClose(-1);
    }

    private void scheduleClose(long timeoutMS) throws InterruptedException {
        CountDownLatch closedLatch = new CountDownLatch(1);
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        executor.schedule(() -> {
            try {
                if (timeoutMS < 0) {
                    connection.closeNow();
                    connection.closeNow();
                    connection.closeNow();
                } else if (timeoutMS == 0) {
                    connection.close();
                    connection.close();
                    connection.close();
                } else {
                    connection.close(timeoutMS);
                    connection.close(timeoutMS);
                    connection.close(timeoutMS);
                }
            } catch (Exception e) {
                ex = e;
            }
            closedLatch.countDown();
        }, 3000, TimeUnit.MILLISECONDS);
        super.sendEvents();
        closedLatch.await();
        Assert.assertNull("Should not receive any exceptions when closing connection.", ex);
        Assert.assertTrue("Connection object should be closed.", connection.isClosed());
    }
    
    @Override
    protected boolean shouldSendThrowException() {
        return true;
    }

    protected boolean isExpectedSendException(Exception e) {
        return e instanceof HecConnectionStateException
            && ((HecConnectionStateException)e).getType() == HecConnectionStateException.Type.SEND_ON_CLOSED_CONNECTION;
    }
    
    @Override
    protected Properties getProps() {
        Properties p = super.getProps();

        p.setProperty(PropertyKeys.EVENT_BATCH_SIZE, "0"); // make all batches don't get sent before Splunk can restart
        p.setProperty(PropertyKeys.ENABLE_CHECKPOINTS, "true");
        p.setProperty(PropertyKeys.MOCK_HTTP_KEY, "true");

        return p;
    }

    
    @Override
    protected int getNumEventsToSend() {
        // we don't expect to finish sending this many events
        return 100000000;
    }

    @Override
    protected BasicCallbacks getCallbacks() {
        return new BasicCallbacks(getNumEventsToSend()) {
            // this test DOES NOT CARE whether or not we get failures or warnings or if we got all checkpoints 
            // since the behavior is non-deterministic
            @Override
            public void await(long timeout, TimeUnit u) throws InterruptedException {
                // no-op
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
