package com.splunk.cloudfwd.test.mock;

import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.error.HecConnectionStateException;

import java.util.Properties;

import static com.splunk.cloudfwd.error.HecConnectionStateException.Type.CONFIGURATION_EXCEPTION;
import com.splunk.cloudfwd.error.HecMaxRetriesException;

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
        props.put(PropertyKeys.MOCK_HTTP_CLASSNAME, "com.splunk.cloudfwd.impl.sim.errorgen.unknownhost.UnknownHostEndpoints");
        return props;
    }

    protected boolean isExpectedConnInstantiationException(Exception e) {
        if (e instanceof HecMaxRetriesException) {
            return  e.getMessage().equals("Simulated UnknownHostException");
        }
        return false;
    }
}
