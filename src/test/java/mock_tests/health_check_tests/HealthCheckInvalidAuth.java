package mock_tests.health_check_tests;

import com.splunk.cloudfwd.ConnectionSettings;
import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import com.splunk.cloudfwd.impl.util.PropertiesFileHelper;
import mock_tests.health_check_tests.AbstractHealthCheckTest;
import org.junit.Test;

import java.util.Properties;
import java.util.concurrent.TimeoutException;

import static com.splunk.cloudfwd.LifecycleEvent.Type.INVALID_AUTH;
import static com.splunk.cloudfwd.PropertyKeys.BLOCKING_TIMEOUT_MS;
import static com.splunk.cloudfwd.PropertyKeys.MOCK_HTTP_CLASSNAME;

/**
 * Created by mhora on 10/4/17.
 */
public class HealthCheckInvalidAuth extends AbstractHealthCheckTest {

    @Override
    protected void setProps(PropertiesFileHelper settings) {
        settings.setMockHttpClassname("com.splunk.cloudfwd.impl.sim.errorgen.splunkcheckfailure.InvalidAuthEndpoints");
        settings.setBlockingTimeoutMS(3000);
    }

    @Test
    public void checkInvalidAuth() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        createConnection(INVALID_AUTH);
    }
}
