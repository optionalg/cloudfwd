package mock_tests;

import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import com.splunk.cloudfwd.error.HecMaxRetriesException;
import com.splunk.cloudfwd.error.HecServerErrorResponseException;
import test_utils.AbstractConnectionTest;
import test_utils.BasicCallbacks;
import org.junit.Test;

import java.util.Properties;

/**
 * Created by eprokop on 9/29/17.
 */
public class GatewayTimeoutTest extends AbstractConnectionTest {

    @Test
    public void preFlightOKButEventPostShouldTimeout() throws InterruptedException {
        super.sendEvents();
    }

    @Override
    protected Properties getProps() {
        Properties p = new Properties();
        p.setProperty(PropertyKeys.MOCK_HTTP_CLASSNAME, "com.splunk.cloudfwd.impl.sim.errorgen.unhealthy.EventPostGatewayTimeoutEndpoints");
        p.setProperty(PropertyKeys.BLOCKING_TIMEOUT_MS, "5000");
        p.setProperty(PropertyKeys.ACK_TIMEOUT_MS, "500000");
        return p;
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
            //return e instanceof HecConnectionTimeoutException;
            return e instanceof HecMaxRetriesException;
        }

        @Override
        protected boolean isExpectedWarningType(Exception e){
            return e instanceof HecServerErrorResponseException;
        }
    }
}
