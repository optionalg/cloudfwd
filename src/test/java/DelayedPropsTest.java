import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.Event;
import com.splunk.cloudfwd.HecConnectionTimeoutException;
import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.sim.ValidatePropsDelayEndpoint;

import org.junit.Test;

import java.util.Properties;
import java.util.concurrent.TimeoutException;

/**
 * Created by eprokop on 9/11/17.
 */
public class DelayedPropsTest extends AbstractMutabilityTest {
    private int numEvents = 1000000;
    private int channelDecom = 10000; //10 sec - we don't want test to run too long,
    // but this can't be too small or channels won't get decomissioned at the right time

    @Test
    public void changeToken() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        connection.setHecEndpointType(Connection.HecEndpoint.RAW_EVENTS_ENDPOINT);
        super.eventType = Event.Type.TEXT;
        setPropsOnEndpoint();

        sendSomeEvents(getNumEventsToSend()/2);
        connection.flush();
        sleep(5000); // wait for all acks

        setToken("different-token-value");
        // CHANNELS MUST GET REAPED BETWEEN HERE...
        sleep((long)(channelDecom*1.5));
        // ...AND HERE
        sendSomeEvents(getNumEventsToSend()/2);

        checkAsserts();
        close();
        checkAsserts();
    }

    private void checkAsserts() {
        AssertionError e;
        if ((e = ValidatePropsDelayEndpoint.getAssertionFailures()) != null) {
            throw e;
        }
    }

    private void setPropsOnEndpoint() {
        ValidatePropsDelayEndpoint.TOKEN = connection.getPropertiesFileHelper().getToken();
    }

    private void setToken(String token) {
        connection.setToken(token);
        ValidatePropsDelayEndpoint.TOKEN = connection.getPropertiesFileHelper().getToken();
    }

    @Override
    protected Properties getProps() {
        Properties props = new Properties();
        props.put(PropertyKeys.ACK_TIMEOUT_MS, "1000000"); //we don't want the ack timout kicking in
        props.put(PropertyKeys.UNRESPONSIVE_MS, "-1"); //no dead channel detection
        props.put(PropertyKeys.MOCK_HTTP_KEY, "true");
        // the asserts for this test exist in the endpoint since we must check values server side
        props.put(PropertyKeys.MOCK_HTTP_CLASSNAME, "com.splunk.cloudfwd.sim.ValidatePropsDelayEndpoint");
        props.put(PropertyKeys.CHANNEL_DECOM_MS, Long.toString(channelDecom));
        props.put(PropertyKeys.MAX_TOTAL_CHANNELS, "8");
        props.put(PropertyKeys.CHANNELS_PER_DESTINATION, "4");
        return props;
    }

    protected int getNumEventsToSend() {
        return numEvents;
    }
}
