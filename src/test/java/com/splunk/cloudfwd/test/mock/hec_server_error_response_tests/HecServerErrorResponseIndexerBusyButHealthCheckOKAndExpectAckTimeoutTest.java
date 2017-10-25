package com.splunk.cloudfwd.test.mock.hec_server_error_response_tests;

import com.splunk.cloudfwd.error.HecAcknowledgmentTimeoutException;
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

/**
 * Created by mhora on 10/3/17.
 */

public class HecServerErrorResponseIndexerBusyButHealthCheckOKAndExpectAckTimeoutTest extends AbstractHecServerErrorResponseTest {
    private static final Logger LOG = LoggerFactory.getLogger(HecServerErrorResponseInvalidTokenTest.class.getName());

    protected int getNumEventsToSend() {
        return 1;
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
                boolean isExpectedType = e instanceof HecAcknowledgmentTimeoutException;
                return isExpectedType;
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
    protected Properties getProps() {
        Properties props = new Properties();
        props.put(MOCK_HTTP_CLASSNAME,
                "com.splunk.cloudfwd.impl.sim.errorgen.unhealthy.EventPostIndexerBusyEndpoints");
        props.put(ACK_TIMEOUT_MS, "2000");  //in this case we expect to see HecConnectionTimeoutException
        props.put(BLOCKING_TIMEOUT_MS, "5000");
        return props;
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
        LOG.info("TESTING INDEXER_BUSY_POST with HecAcknowledgementTimeoutException expected");
        createConnection();
        super.sendEvents();
        connection.closeNow();
    }

}