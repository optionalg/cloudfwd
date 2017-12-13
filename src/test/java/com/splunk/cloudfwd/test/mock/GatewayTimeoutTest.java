package com.splunk.cloudfwd.test.mock;

import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import com.splunk.cloudfwd.error.HecMaxRetriesException;
import com.splunk.cloudfwd.error.HecServerErrorResponseException;
import com.splunk.cloudfwd.test.util.AbstractConnectionTest;
import com.splunk.cloudfwd.test.util.BasicCallbacks;
import com.splunk.cloudfwd.impl.util.PropertiesFileHelper;
import org.junit.Test;

/**
 * Created by eprokop on 9/29/17.
 */
public class GatewayTimeoutTest extends AbstractConnectionTest {

    @Test
    public void preFlightOKButEventPostShouldTimeout() throws InterruptedException {
        super.sendEvents();
    }

    @Override
    protected void configureProps(PropertiesFileHelper settings) {
        settings.setMockHttpClassname("com.splunk.cloudfwd.impl.sim.errorgen.unhealthy.EventPostGatewayTimeoutEndpoints");
        settings.setBlockingTimeoutMS(5000);
        settings.setAckTimeoutMS(500000);
    }

    @Override
    protected int getNumEventsToSend() {
        return 3;
    }

    @Override
    protected BasicCallbacks getCallbacks() {
        return new EventPostGatewayTimeoutCallbacks(getNumEventsToSend());
    }

    private class EventPostGatewayTimeoutCallbacks extends BasicCallbacks {

        public EventPostGatewayTimeoutCallbacks(int expected) {
            super(expected);
        }

        @Override
        public boolean shouldFail(){
            return true;
        }

        @Override
        protected boolean isExpectedFailureType(Exception e) {
            // connection.close() will cause the events to get orphaned in
            // the load balancer and timeout since all channels will be closed
            return e instanceof HecMaxRetriesException || e instanceof HecConnectionTimeoutException;
        }

        @Override
        protected boolean isExpectedWarningType(Exception e){
            return e instanceof HecServerErrorResponseException;
        }
    }
}
