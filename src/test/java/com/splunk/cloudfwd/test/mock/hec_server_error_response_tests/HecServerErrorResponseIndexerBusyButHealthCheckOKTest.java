package com.splunk.cloudfwd.test.mock.hec_server_error_response_tests;

import com.splunk.cloudfwd.ConnectionSettings;
import com.splunk.cloudfwd.error.HecConnectionStateException;
import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import com.splunk.cloudfwd.error.HecServerErrorResponseException;
import com.splunk.cloudfwd.test.util.BasicCallbacks;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeoutException;

import static com.splunk.cloudfwd.LifecycleEvent.Type.INDEXER_BUSY;
import static com.splunk.cloudfwd.PropertyKeys.ACK_TIMEOUT_MS;
import static com.splunk.cloudfwd.PropertyKeys.BLOCKING_TIMEOUT_MS;
import static com.splunk.cloudfwd.PropertyKeys.MOCK_HTTP_CLASSNAME;
import com.splunk.cloudfwd.error.HecMaxRetriesException;

/**
 * Created by mhora on 10/3/17.
 */

public class HecServerErrorResponseIndexerBusyButHealthCheckOKTest extends AbstractHecServerErrorResponseTest {
    private static final Logger LOG = LoggerFactory.getLogger(HecServerErrorResponseIndexerBusyButHealthCheckOKTest.class.getName());

    protected int getNumEventsToSend() {
        return 3;
    }

    @Override
    protected BasicCallbacks getCallbacks() {
        return new BasicCallbacks(getNumEventsToSend()) {
            @Override
            public boolean shouldFail() {
                return true;
            }

            @Override
            protected boolean isExpectedFailureType(Exception e) {
                return e instanceof HecConnectionTimeoutException || e instanceof HecMaxRetriesException;
            }

            @Override
            protected boolean isExpectedWarningType(Exception e) {
                boolean isExpected = e instanceof HecServerErrorResponseException
                        && ((HecServerErrorResponseException)e).getLifecycleType() == INDEXER_BUSY;
                return isExpected;
            }

            @Override
            public boolean shouldWarn(){
                return true;
            }
        };
    }

    @Override
    protected ConnectionSettings getTestProps() {
        ConnectionSettings settings = super.getTestProps();
        settings.setMockHttpClassname(
                "com.splunk.cloudfwd.impl.sim.errorgen.unhealthy.EventPostIndexerBusyEndpoints");
        settings.setAckTimeoutMS(500000);//in this case we expect to see HecConnectionTimeoutException
        settings.setBlockingTimeoutMS(5000);
        settings.setMaxTotalChannels(1);
        settings.setMaxRetries(1);
        return settings;
    }

    @Override
    protected boolean isExpectedSendException(Exception e) {
        return false;
    }

    @Override
    protected boolean shouldSendThrowException() {
        return false;
    }

    @Test
    public void postToBusyIndexerButHealthCheckOK() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        LOG.info("TESTING INDEXER_BUSY_POST with HecConnectionTimeoutException expected");
        createConnection();
        super.sendEvents();
    }

}