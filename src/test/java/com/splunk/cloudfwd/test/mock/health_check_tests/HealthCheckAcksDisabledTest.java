package com.splunk.cloudfwd.test.mock.health_check_tests;

import com.splunk.cloudfwd.ConnectionSettings;
import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import com.splunk.cloudfwd.error.HecMaxRetriesException;
import com.splunk.cloudfwd.error.HecServerErrorResponseException;
import com.splunk.cloudfwd.test.mock.connection_doesnt_throw_exception.AbstractExceptionOnSendTest;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeoutException;

import static com.splunk.cloudfwd.LifecycleEvent.Type.ACK_DISABLED;
import static com.splunk.cloudfwd.PropertyKeys.BLOCKING_TIMEOUT_MS;
import static com.splunk.cloudfwd.PropertyKeys.MOCK_HTTP_CLASSNAME;

/**
 * Created by mhora on 10/4/17.
 */
public class HealthCheckAcksDisabledTest extends AbstractExceptionOnSendTest {
    private static final Logger LOG = LoggerFactory.getLogger(HealthCheckAcksDisabledTest.class.getName());

    @Override
    protected ConnectionSettings getTestProps() {
        ConnectionSettings settings = super.getTestProps();
        settings.setMockHttpClassname(
           "com.splunk.cloudfwd.impl.sim.errorgen.splunkcheckfailure.AckDisabledEndpoints");
        settings.setBlockingTimeoutMS(3000);
        return settings;
    }

    @Test
    public void checkAcksDisabled() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        super.sendEvents(false, false);
        assertAllChannelsFailed(HecServerErrorResponseException.class, "HecServerErrorResponseException{serverRespObject=HecErrorResponseValueObject{text=ACK is disabled, code=14, invalidEventNumber=-1}, httpBodyAndStatus=HttpBodyAndStatus{statusCode=400, body={\"text\":\"ACK is disabled\",\"code\":14}}, lifecycleType=ACK_DISABLED, url=https://127.0.0.1:8088, errorType=RECOVERABLE_CONFIG_ERROR, context=null}");
    }
}
