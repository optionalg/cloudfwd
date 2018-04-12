package com.splunk.cloudfwd.test.mock.health_check_tests;

import com.splunk.cloudfwd.ConnectionSettings;
import com.splunk.cloudfwd.LifecycleEvent;
import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeoutException;

/**
 * Created by mhora on 10/4/17.
 */
public class HealthCheckInDetentionConnThrowsTest extends AbstractHealthCheckTest {
    private static final Logger LOG = LoggerFactory.getLogger(HealthCheckInDetentionTest.class.getName());

    @Override
    protected void configureProps(ConnectionSettings settings) {
        settings.setConntctionThrowsExceptionOnCreation(true);
        settings.setMockHttpClassname("com.splunk.cloudfwd.impl.sim.errorgen.indexer.InDetentionEndpoints");
        settings.setBlockingTimeoutMS(3000);
    }

    @Test
    public void checkInDetention() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        createConnection(LifecycleEvent.Type.INDEXER_IN_DETENTION);
    }
}
