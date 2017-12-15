package com.splunk.cloudfwd.test.mock.health_check_tests;

import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import com.splunk.cloudfwd.error.HecServerErrorResponseException;
import com.splunk.cloudfwd.test.mock.AbstractExceptionOnSendTest;
import com.splunk.cloudfwd.test.mock.health_check_tests.AbstractHealthCheckTest;
import org.junit.Test;

import java.util.Properties;
import java.util.concurrent.TimeoutException;

import static com.splunk.cloudfwd.LifecycleEvent.Type.INVALID_TOKEN;
import static com.splunk.cloudfwd.PropertyKeys.BLOCKING_TIMEOUT_MS;
import static com.splunk.cloudfwd.PropertyKeys.MOCK_HTTP_CLASSNAME;

/**
 * Created by mhora on 10/4/17.
 */
public class HealthCheckInvalidTokenTest extends AbstractExceptionOnSendTest{
    
    @Test
    public void checkInvalidToken() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        super.sendEvents(false, false);
        assertAllChannelsFailed(HecServerErrorResponseException.class,
                "HecServerErrorResponseException{serverRespObject=HecErrorResponseValueObject{text=Invalid token, code=4, invalidEventNumber=-1}, " +
                        "httpBodyAndStatus=HttpBodyAndStatus{statusCode=403, body={\"text\":\"Invalid token\",\"code\":4}}, " +
                        "lifecycleType=INVALID_TOKEN, url=https://127.0.0.1:8088, errorType=RECOVERABLE_CONFIG_ERROR, context=null}");}
    
    @Override
    protected Properties getProps() {
        Properties props = new Properties();
        props.put(MOCK_HTTP_CLASSNAME,
            "com.splunk.cloudfwd.impl.sim.errorgen.splunkcheckfailure.InvalidTokenEndpoints");
        props.put(BLOCKING_TIMEOUT_MS, "500");
        return props;
    }
    
}
