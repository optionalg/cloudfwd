package mock_tests;

import com.splunk.cloudfwd.ConnectionSettings;
import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import com.splunk.cloudfwd.error.HecServerErrorResponseException;
import com.splunk.cloudfwd.impl.util.PropertiesFileHelper;
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
    protected void setProps(PropertiesFileHelper settings) {
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
            return e instanceof HecConnectionTimeoutException;
        }

        @Override
        protected boolean isExpectedWarningType(Exception e){
            return e instanceof HecServerErrorResponseException;
        }
    }
}
