package com.splunk.cloudfwd.test.mock.hec_server_error_response_tests;

import com.splunk.cloudfwd.ConnectionSettings;
import com.splunk.cloudfwd.LifecycleEvent;
import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import com.splunk.cloudfwd.error.HecServerErrorResponseException;
import com.splunk.cloudfwd.test.mock.connection_doesnt_throw_exception.AbstractExceptionOnSendTest;
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

public class HecServerErrorResponseAcksDisabledTest extends AbstractExceptionOnSendTest {
    private static final Logger LOG = LoggerFactory.getLogger(HecServerErrorResponseAcksDisabledTest.class.getName());

    protected int getNumEventsToSend() {
        return 3;
    }
    
    @Override
    protected ConnectionSettings getTestProps() {
        ConnectionSettings settings = super.getTestProps();
        settings.setMockHttpClassname(
                "com.splunk.cloudfwd.impl.sim.errorgen.splunkcheckfailure.AckDisabledEndpoints");
        settings.setBlockingTimeoutMS(500);
        return settings;
    }
    
    //pre-flight check NOT ok
    @Test
    public void sendWithAcksDisabled() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        super.sendEvents(false, false);
        assertAllChannelsFailed(HecServerErrorResponseException.class,
                "HecServerErrorResponseException{serverRespObject=HecErrorResponseValueObject{text=ACK is disabled, code=14, invalidEventNumber=-1}, " +
                        "httpBodyAndStatus=HttpBodyAndStatus{statusCode=400, body={\"text\":\"ACK is disabled\",\"code\":14}}, " +
                        "lifecycleType=ACK_DISABLED, url=https://127.0.0.1:8088, errorType=RECOVERABLE_CONFIG_ERROR, context=null}");}
    
}
