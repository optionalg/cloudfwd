package com.splunk.cloudfwd.test.mock.hec_server_error_response_tests;

import com.splunk.cloudfwd.LifecycleEvent;
import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import com.splunk.cloudfwd.error.HecServerErrorResponseException;
import com.splunk.cloudfwd.test.util.BasicCallbacks;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import static com.splunk.cloudfwd.PropertyKeys.ACK_TIMEOUT_MS;
import static com.splunk.cloudfwd.PropertyKeys.BLOCKING_TIMEOUT_MS;
import static com.splunk.cloudfwd.PropertyKeys.MOCK_HTTP_CLASSNAME;

/**
 * Created by mhora on 10/3/17.
 */
public class HecServerErrorResponseInvalidTokenTest extends AbstractHecServerErrorResponseTest {
    private static final Logger LOG = LoggerFactory.getLogger(HecServerErrorResponseInvalidTokenTest.class.getName());

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
                boolean isExpectedType = e instanceof HecServerErrorResponseException
                        && ((HecServerErrorResponseException) e).getCode() == 4;
                return isExpectedType;

            }
        };
    }

    @Override
    protected Properties getProps() {
        Properties props = super.getProps();
        props.put(MOCK_HTTP_CLASSNAME,
                "com.splunk.cloudfwd.impl.sim.errorgen.splunkcheckfailure.InvalidTokenEndpoints");
        props.put(ACK_TIMEOUT_MS, "500000");  //in this case we expect to see HecConnectionTimeoutException
        props.put(BLOCKING_TIMEOUT_MS, "5000");
        return props;
    }

    @Override
    protected boolean isExpectedSendException(Exception e) {
        boolean isExpected = false;
        if (e instanceof HecConnectionTimeoutException) {
            isExpected = true;
        }
        return isExpected;
    }

    @Override
    protected boolean shouldSendThrowException() {
        return true;
    }

    //pre-flight check NOT ok
    @Test
    public void sendToInvalidToken() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        LOG.info("TESTING INVALID_TOKEN");
        createConnection(LifecycleEvent.Type.INVALID_TOKEN);
    }

}
