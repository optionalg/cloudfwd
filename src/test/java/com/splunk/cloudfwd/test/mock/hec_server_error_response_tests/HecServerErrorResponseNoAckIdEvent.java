package com.splunk.cloudfwd.test.mock.hec_server_error_response_tests;

import com.splunk.cloudfwd.ConnectionSettings;
import com.splunk.cloudfwd.error.HecConnectionStateException;
import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import com.splunk.cloudfwd.test.util.BasicCallbacks;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeoutException;

import static com.splunk.cloudfwd.error.HecConnectionStateException.Type.CONFIGURATION_EXCEPTION;

/**
 * Created by mhora on 10/3/17.
 */
public class HecServerErrorResponseNoAckIdEvent extends AbstractHecServerErrorResponseTest {
    private static final Logger LOG = LoggerFactory.getLogger(HecServerErrorResponseNoAckIdEvent.class.getName());

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
                boolean isExpectedType = e instanceof HecConnectionStateException
                        && ((HecConnectionStateException) e).getType() == CONFIGURATION_EXCEPTION;
                return isExpectedType;
            }

            @Override
            protected boolean isExpectedWarningType(Exception e) {
                return false;
            }

            @Override
            public boolean shouldWarn(){
                return false;
            }
        };
    }

    @Override
    protected void configureProps(ConnectionSettings settings) {
        //in this case, the pre-flight check will pass, and we are simulating were we detect acks disabled on event post
        settings.setMockHttpClassname("com.splunk.cloudfwd.impl.sim.errorgen.unhealthy.EventPostNoAckIdEndpoints");
        settings.setAckTimeoutMS(500000); //in this case we excpect to see HecConnectionTimeoutException
        settings.setBlockingTimeoutMS(5000);
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
    public void postNoAckIdEvent() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        LOG.info("TESTING ACK_ID_DISABLED_AFTER_PREFLIGHT_SUCCEEDS");
        createConnection();
        super.sendEvents();
    }
}

