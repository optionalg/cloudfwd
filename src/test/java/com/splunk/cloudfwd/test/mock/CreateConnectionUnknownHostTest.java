package com.splunk.cloudfwd.test.mock;

import com.splunk.cloudfwd.ConnectionSettings;
import com.splunk.cloudfwd.error.HecMaxRetriesException;

/**
 * Scenario: Unknown host provided (no "good" URLs)
 * Expected behavior: Connection fails to instantiate and throws expected exception
 *
 * Created by eprokop on 10/5/17.
 */
public class CreateConnectionUnknownHostTest extends ExceptionConnInstantiationTest {
    @Override
    protected void configureProps(ConnectionSettings settings) {
        settings.setUrls("https://foobarunknownhostbaz:8088");
        settings.setMockHttpClassname("com.splunk.cloudfwd.impl.sim.errorgen.unknownhost.UnknownHostEndpoints");
    }

    protected boolean isExpectedConnInstantiationException(Exception e) {
        if (e instanceof HecMaxRetriesException) {
            return  e.getMessage().equals("Simulated UnknownHostException");
        }
        return false;
    }
}



