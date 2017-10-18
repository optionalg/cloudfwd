package mock_tests;

import com.splunk.cloudfwd.ConnectionSettings;
import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.error.HecConnectionStateException;
import com.splunk.cloudfwd.impl.util.PropertiesFileHelper;

import java.net.UnknownHostException;
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
    protected void setProps(PropertiesFileHelper settings) {
        try {
            settings.setUrls("https://foobarunknownhostbaz:8088");
        } catch(UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    protected boolean isExpectedConnInstantiationException(Exception e) {
        if (e instanceof HecConnectionStateException) {
            return ((HecConnectionStateException)e).getType() == CONFIGURATION_EXCEPTION
                && e.getMessage().equals("Could not resolve any host names.");
        }
        return false;
    }
}
