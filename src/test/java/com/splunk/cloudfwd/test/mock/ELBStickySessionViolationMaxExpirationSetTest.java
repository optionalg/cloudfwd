package com.splunk.cloudfwd.test.mock;

import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.ConnectionSettings;
import com.splunk.cloudfwd.Event;
import com.splunk.cloudfwd.error.HecMaxRetriesException;
import com.splunk.cloudfwd.error.HecNoValidChannelsException;
import com.splunk.cloudfwd.error.HecNonStickySessionException;
import com.splunk.cloudfwd.impl.sim.errorgen.cookies.UpdateableELBCookieEndpoints;
import com.splunk.cloudfwd.impl.util.HecHealthImpl;
import com.splunk.cloudfwd.test.mock.connection_doesnt_throw_exception.AbstractExceptionOnSendTest;
import com.splunk.cloudfwd.test.util.AbstractConnectionTest;
import com.splunk.cloudfwd.test.util.BasicCallbacks;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;


public class ELBStickySessionViolationMaxExpirationSetTest extends AbstractExceptionOnSendTest {

    private static final Logger LOG = LoggerFactory.getLogger(ELBStickySessionViolationMaxExpirationSetTest.class.getName());
    
    
    
    @Override
    @Before
    public void setUp() {
        UpdateableELBCookieEndpoints.setMaxAge(1);
        super.setUp();
    }
    
    @Test
    /**
     * Test sends events to an endpoint and periodically changes sticky session 
     * on the endpoint. 
     * Cloudfwd should 
     *   * detect ELB cookie change on a channel
     *   * fail events in flight with HecNonStickySessionException exception 
     */
    public void testWithSessionCookies() throws InterruptedException {
        sendEvents(false, false);
        LOG.debug("getHealth: " + connection.getHealth());
        assertAllChannelsFailedWithRegex(HecMaxRetriesException.class, "Caused by: " +
            "com.splunk.cloudfwd.error.HecNonStickySessionException, with message: " +
            "Received an HTTP Response with a cookie with an expiration time enabled.*");
    }

    @Override
    protected int getNumEventsToSend() {
        return 1;
    }

    @Override
    protected void configureProps(ConnectionSettings settings) {
        settings.setMockHttpClassname("com.splunk.cloudfwd.impl.sim.errorgen.cookies.UpdateableELBCookieEndpoints");
        settings.setMaxTotalChannels(1);
        settings.setBlockingTimeoutMS(5000);
        settings.setMaxRetries(1);
        settings.setAckTimeoutMS(10000);
        settings.setPreFlightTimeoutMS(3000);
        settings.setNonStickyChannelReplacementDelayMs(1000);
    }

//    @Override
//    protected BasicCallbacks getCallbacks() {
//        return new BasicCallbacks(getNumEventsToSend()) {
//            @Override
//            protected boolean isExpectedFailureType(Exception e){
//                LOG.debug("isExpectedFailureType: e={}", e.toString());
//                return (e instanceof HecNoValidChannelsException);
//            }
//
//        };
//    }

    @Override
    protected boolean shouldSendThrowException() {return true;}

    @Override
    protected boolean isExpectedSendException(Exception e) {
        LOG.debug("isExpectedSendException: e={}", e.toString());
        return (e instanceof HecNoValidChannelsException);
    }

}