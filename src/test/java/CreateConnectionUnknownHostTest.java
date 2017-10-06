import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.error.HecConnectionStateException;

import java.util.Properties;

import static com.splunk.cloudfwd.error.HecConnectionStateException.Type.CONFIGURATION_EXCEPTION;

/**
 * Scenario: Unknown host provided (no "good" URLs)
 * Expected behavior: Connection fails to instantiate and throws expected exception
 *
 * Created by eprokop on 10/5/17.
 */
public class CreateConnectionUnknownHostTest extends ExceptionConnInstantiationTest {
    @Override
    protected Properties getProps() {
        Properties props = new Properties();
        props.put(PropertyKeys.COLLECTOR_URI, "https://foobarunknownhostbaz:8088");
        return props;
    }

    protected boolean isExpectedConnInstantiationException(Exception e) {
        if (e instanceof HecConnectionStateException) {
            return ((HecConnectionStateException)e).getType() == CONFIGURATION_EXCEPTION
                && e.getMessage().equals("Could not resolve any host names.");
        }
        return false;
    }
}
