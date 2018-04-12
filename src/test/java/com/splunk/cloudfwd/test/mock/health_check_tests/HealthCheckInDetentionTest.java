package com.splunk.cloudfwd.test.mock.health_check_tests;

import com.splunk.cloudfwd.ConnectionSettings;
import com.splunk.cloudfwd.LifecycleEvent;
import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import com.splunk.cloudfwd.error.HecNoValidChannelsException;
import com.splunk.cloudfwd.error.HecServerErrorResponseException;
import com.splunk.cloudfwd.test.mock.connection_doesnt_throw_exception.AbstractExceptionOnSendTest;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeoutException;

import static com.splunk.cloudfwd.PropertyKeys.*;

/**
 * Created by mhora on 10/4/17.
 */
public class HealthCheckInDetentionTest extends AbstractExceptionOnSendTest {
    private static final Logger LOG = LoggerFactory.getLogger(HealthCheckInDetentionTest.class.getName());

    @Test
    public void checkInDetention() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        super.sendEvents(false, false);
        assertAllChannelsFailed(HecServerErrorResponseException.class,
                "HecServerErrorResponseException" +
                        "{serverRespObject=HecErrorResponseValueObject{text=null, code=-1, invalidEventNumber=-1}, " +
                        "httpBodyAndStatus=HttpBodyAndStatus{statusCode=404, body=Not Found}, " +
                        "lifecycleType=INDEXER_IN_DETENTION, url=https://127.0.0.1:8088, " +
                        "errorType=RECOVERABLE_CONFIG_ERROR, context=null}");}
    
    @Override
    protected ConnectionSettings getTestProps() {
        ConnectionSettings settings = super.getTestProps();
        settings.setMockHttpClassname(
                "com.splunk.cloudfwd.impl.sim.errorgen.indexer.InDetentionEndpoints");
        settings.setBlockingTimeoutMS(500);
        return settings;
    }
    
    @Override
    protected boolean isExpectedSendException(Exception ex) {
        if (ex instanceof HecConnectionTimeoutException) return true;
        return false;
    }
}
