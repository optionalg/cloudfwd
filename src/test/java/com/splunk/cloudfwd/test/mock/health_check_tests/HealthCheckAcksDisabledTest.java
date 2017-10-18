package com.splunk.cloudfwd.test.mock.health_check_tests;

import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
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
public class HealthCheckAcksDisabledTest extends AbstractHealthCheckTest {
    private static final Logger LOG = LoggerFactory.getLogger(HealthCheckAcksDisabledTest.class.getName());

    @Override
    protected Properties getProps() {
        Properties props = new Properties();
        props.put(MOCK_HTTP_CLASSNAME,
           "com.splunk.cloudfwd.impl.sim.errorgen.splunkcheckfailure.AckDisabledEndpoints");
        props.put(BLOCKING_TIMEOUT_MS, "3000");
        return props;
    }

    @Test
    public void checkAcksDisabled() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        createConnection(ACK_DISABLED);
    }
}
