package com.splunk.cloudfwd.test.mock.health_check_tests;

import com.splunk.cloudfwd.ConnectionSettings;
import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import org.junit.Test;

import java.util.concurrent.TimeoutException;

import static com.splunk.cloudfwd.LifecycleEvent.Type.INVALID_AUTH;

/**
 * Created by mhora on 10/4/17.
 */
public class HealthCheckInvalidAuth extends AbstractHealthCheckTest {

    @Override
    protected void configureProps(ConnectionSettings settings) {
        settings.setMockHttpClassname("com.splunk.cloudfwd.impl.sim.errorgen.splunkcheckfailure.InvalidAuthEndpoints");
        settings.setBlockingTimeoutMS(3000);
    }

    @Test
    public void checkInvalidAuth() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        createConnection(INVALID_AUTH);
    }
}
