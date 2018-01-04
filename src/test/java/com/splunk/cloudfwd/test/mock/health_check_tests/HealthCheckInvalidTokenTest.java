package com.splunk.cloudfwd.test.mock.health_check_tests;

import com.splunk.cloudfwd.ConnectionSettings;
import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import org.junit.Test;

import java.util.concurrent.TimeoutException;

import static com.splunk.cloudfwd.LifecycleEvent.Type.INVALID_TOKEN;

/**
 * Created by mhora on 10/4/17.
 */
public class HealthCheckInvalidTokenTest extends AbstractHealthCheckTest {

    @Override
    protected void configureProps(ConnectionSettings settings) {
        settings.setMockHttpClassname("com.splunk.cloudfwd.impl.sim.errorgen.splunkcheckfailure.InvalidTokenEndpoints");
        settings.setBlockingTimeoutMS(3000);
    }

    @Test
    public void checkInvalidToken() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        createConnection(INVALID_TOKEN);
    }
}
