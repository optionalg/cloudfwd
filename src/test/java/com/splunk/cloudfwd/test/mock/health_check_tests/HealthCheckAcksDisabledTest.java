package com.splunk.cloudfwd.test.mock.health_check_tests;

import com.splunk.cloudfwd.ConnectionSettings;
import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeoutException;

import static com.splunk.cloudfwd.LifecycleEvent.Type.ACK_DISABLED;

/**
 * Created by mhora on 10/4/17.
 */
public class HealthCheckAcksDisabledTest extends AbstractHealthCheckTest {
    private static final Logger LOG = LoggerFactory.getLogger(HealthCheckAcksDisabledTest.class.getName());

    @Override
    protected void configureProps(ConnectionSettings settings) {
        settings.setMockHttpClassname("com.splunk.cloudfwd.impl.sim.errorgen.splunkcheckfailure.AckDisabledEndpoints");
        settings.setBlockingTimeoutMS(3000);
    }

    @Test
    public void checkAcksDisabled() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        createConnection(ACK_DISABLED);
    }
}
