package com.splunk.cloudfwd.test.mock.health_check_tests;

import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import com.splunk.cloudfwd.error.HecServerErrorResponseException;
import com.splunk.cloudfwd.test.mock.AbstractExceptionOnSendTest;
import com.splunk.cloudfwd.test.mock.health_check_tests.AbstractHealthCheckTest;
import org.junit.Test;

import java.util.Properties;
import java.util.concurrent.TimeoutException;

import static com.splunk.cloudfwd.LifecycleEvent.Type.INVALID_AUTH;
import static com.splunk.cloudfwd.PropertyKeys.BLOCKING_TIMEOUT_MS;
import static com.splunk.cloudfwd.PropertyKeys.MOCK_HTTP_CLASSNAME;

/**
 * Created by mhora on 10/4/17.
 */
public class HealthCheckInvalidAuth extends AbstractExceptionOnSendTest {

    @Test
    public void checkInvalidAuth() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        super.sendEvents(false, false);
        assertAllChannelsFailed(HecServerErrorResponseException.class,
                "HecServerErrorResponseException" +
                        "{serverRespObject=HecErrorResponseValueObject{text=Invalid authorization, code=3, invalidEventNumber=-1}, " +
                        "httpBodyAndStatus=HttpBodyAndStatus{statusCode=401, body={\"text\":\"Invalid authorization\",\"code\":3}}, " +
                        "lifecycleType=INVALID_AUTH, url=https://127.0.0.1:8088, errorType=NON_RECOVERABLE_ERROR, context=null}");}
    
    @Override
    protected Properties getProps() {
        Properties props = new Properties();
        props.put(MOCK_HTTP_CLASSNAME,
            "com.splunk.cloudfwd.impl.sim.errorgen.splunkcheckfailure.InvalidAuthEndpoints");
        props.put(BLOCKING_TIMEOUT_MS, "500");
        return props;
    }
    
    @Override
    protected boolean isExpectedSendException(Exception ex) {
        if (ex instanceof HecConnectionTimeoutException) return true;
        return false;
    }
}
