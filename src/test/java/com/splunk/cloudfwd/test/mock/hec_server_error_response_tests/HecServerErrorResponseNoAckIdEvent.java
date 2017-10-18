package com.splunk.cloudfwd.test.mock.hec_server_error_response_tests;

import com.splunk.cloudfwd.error.HecConnectionStateException;
import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import com.splunk.cloudfwd.test.util.BasicCallbacks;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeoutException;

import static com.splunk.cloudfwd.PropertyKeys.ACK_TIMEOUT_MS;
import static com.splunk.cloudfwd.PropertyKeys.BLOCKING_TIMEOUT_MS;
import static com.splunk.cloudfwd.PropertyKeys.MOCK_HTTP_CLASSNAME;
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
    protected Properties getProps() {
        Properties props = new Properties();
        //in this case, the pre-flight check will pass, and we are simulating were we detect acks disabled on event post
        props.put(MOCK_HTTP_CLASSNAME,
                "com.splunk.cloudfwd.impl.sim.errorgen.unhealthy.EventPostNoAckIdEndpoints");
        props.put(ACK_TIMEOUT_MS, "500000");  //in this case we excpect to see HecConnectionTimeoutException
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
    public void postNoAckIdEvent() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        LOG.info("TESTING ACK_ID_DISABLED_AFTER_PREFLIGHT_SUCCEEDS");
        createConnection();
        super.sendEvents();
    }
}

