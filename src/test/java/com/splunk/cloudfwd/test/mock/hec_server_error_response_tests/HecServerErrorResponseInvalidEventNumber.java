package com.splunk.cloudfwd.test.mock.hec_server_error_response_tests;

import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.Event;
import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import com.splunk.cloudfwd.error.HecServerErrorResponseException;
import com.splunk.cloudfwd.test.util.BasicCallbacks;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeoutException;

import static com.splunk.cloudfwd.PropertyKeys.ACK_TIMEOUT_MS;
import static com.splunk.cloudfwd.PropertyKeys.BLOCKING_TIMEOUT_MS;
import static com.splunk.cloudfwd.PropertyKeys.MOCK_HTTP_CLASSNAME;

/**
 * Created by mhora on 10/3/17.
 */
public class HecServerErrorResponseInvalidEventNumber extends AbstractHecServerErrorResponseTest {
    private static final Logger LOG = LoggerFactory.getLogger(HecServerErrorResponseNoAckIdEvent.class.getName());

    protected int getNumEventsToSend() {
        return 1;
    }

    @Override
    protected BasicCallbacks getCallbacks() {
        return new BasicCallbacks(getNumEventsToSend()) {
            @Override
            public boolean shouldFail() {
                return true;
            }

            @Override
            protected boolean isExpectedFailureType(Exception e) {
                boolean isExpectedType;
                if(e instanceof HecServerErrorResponseException) {
                    HecServerErrorResponseException srvrEx = (HecServerErrorResponseException) e;
                    Assert.assertEquals("Didn't find code 6", 6, srvrEx.getCode());
                    Assert.assertEquals("Didn't find invalid-event-number 0", 0, srvrEx.getInvalidEventNumber());
                    isExpectedType = true;
                } else {
                    isExpectedType = false;
                }
                return isExpectedType;
            }

            @Override
            protected boolean isExpectedWarningType(Exception e) {
                return false;
            }

            @Override
            public boolean shouldWarn(){
                return false;
            }
        };
    }

    @Override
    protected Properties getProps() {
        Properties props = new Properties();
        props.put(MOCK_HTTP_CLASSNAME,
                "com.splunk.cloudfwd.impl.sim.errorgen.invalidvent.InvalidEventEndpoint");
        props.put(ACK_TIMEOUT_MS, "500000");  //in this case we expect to see HecConnectionTimeoutException
        props.put(BLOCKING_TIMEOUT_MS, "5000");
        return props;
    }

    @Override
    protected boolean isExpectedSendException(Exception e) {
        boolean isExpected = false;
        if (e instanceof HecConnectionTimeoutException) {
            isExpected = true;
        }
        return isExpected;
    }

    @Override
    protected boolean shouldSendThrowException() {
        return false;
    }

    @Override
    protected Event nextEvent(int seqno) {
        //we need to intentionally generate data that does not have "/events" 'envelope'
        return super.getUnvalidatedBytesToRawEndpoint(seqno); //Generate BAD data
    }

    @Test
    public void postInvalidEvent() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        LOG.info("TESTING INVALID_EVENT_NUMBER");
        createConnection();
        connection.getSettings().setHecEndpointType(
                Connection.HecEndpoint.STRUCTURED_EVENTS_ENDPOINT);
        this.eventType = Event.Type.UNKNOWN;
//        try {
        super.sendEvents();
//        } catch (HecConnectionTimeoutException e) {
//            LOG.trace("Got expected timeout exception because all channels are unhealthy "
//                    + "due to indexer being busy (per test design): "
//                    + e.getMessage());
//        }
        // TODO: we are currently not calling any failed callbacks in this case. Do we want to?
    }

}
