import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.Event;
import com.splunk.cloudfwd.HecConnectionTimeoutException;
import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.sim.ValidatePropsLiveEndpoint;
import org.junit.Test;

import java.util.Properties;
import java.util.concurrent.TimeoutException;

/**
 * Created by eprokop on 9/11/17.
 */
public class LivePropsTest extends AbstractMutabilityTest {
    private int numEvents = 1000000;

    @Test
    public void changeEndpointType() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        setPropsOnEndpoint();
        connection.setHecEndpointType(Connection.HecEndpoint.RAW_EVENTS_ENDPOINT);
        super.eventType = Event.Type.TEXT;
        sendSomeEvents(getNumEventsToSend()/4);
        connection.flush();

        connection.setHecEndpointType(Connection.HecEndpoint.STRUCTURED_EVENTS_ENDPOINT);
        super.eventType = Event.Type.TEXT;
        sendSomeEvents(getNumEventsToSend()/4);
        connection.flush();

        connection.setHecEndpointType(Connection.HecEndpoint.RAW_EVENTS_ENDPOINT);
        super.eventType = Event.Type.UNKNOWN;
        sendSomeEvents(getNumEventsToSend()/4);
        connection.flush();

        connection.setHecEndpointType(Connection.HecEndpoint.STRUCTURED_EVENTS_ENDPOINT);
        super.eventType = Event.Type.UNKNOWN;
        sendSomeEvents(getNumEventsToSend()/4);

        close();
    }

    @Test
    public void changeUrlsAndAckTimeout() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        connection.setHecEndpointType(Connection.HecEndpoint.RAW_EVENTS_ENDPOINT);
        super.eventType = Event.Type.TEXT;
        setPropsOnEndpoint();
        sendSomeEvents(getNumEventsToSend()/4);
        sleep(2000); // let ack polling finish

        setUrls("https://127.0.0.1:8188");
        setAckTimeout(120000);

        sendSomeEvents(getNumEventsToSend()/4);
        sleep(2000);

        setAckTimeout(65000);
        setUrls("https://127.0.0.1:8288, https://127.0.0.1:8388");

        sendSomeEvents(getNumEventsToSend()/4);
        sleep(2000);

        setUrls("https://127.0.0.1:8488, https://127.0.0.1:8588, https://127.0.0.1:8688");
        setAckTimeout(80000);

        sendSomeEvents(getNumEventsToSend()/4);
        close();
    }

    @Override
    protected Properties getProps() {
        Properties props = new Properties();
        props.put(PropertyKeys.ACK_TIMEOUT_MS, "1000000"); //we don't want the ack timout kicking in
        props.put(PropertyKeys.UNRESPONSIVE_MS, "-1"); //no dead channel detection
        props.put(PropertyKeys.MOCK_HTTP_KEY, "true");
        // the asserts for this test exist in the endpoint since we must check values server side
        props.put(PropertyKeys.MOCK_HTTP_CLASSNAME, "com.splunk.cloudfwd.sim.ValidatePropsLiveEndpoint");
        return props;
    }

    private void setPropsOnEndpoint() {
        ValidatePropsLiveEndpoint.URLS = connection.getPropertiesFileHelper().getUrls();
        ValidatePropsLiveEndpoint.ACK_TIMEOUT_MS = connection.getPropertiesFileHelper().getAckTimeoutMS();
    }

    protected int getNumEventsToSend() {
        return numEvents;
    }

    private void setUrls(String urls) {
        connection.setUrls(urls);
        ValidatePropsLiveEndpoint.URLS = connection.getPropertiesFileHelper().getUrls();
    }

    private void setAckTimeout(long ms) {
        connection.setEventAcknowledgementTimeoutMS(ms);
        ValidatePropsLiveEndpoint.ACK_TIMEOUT_MS = connection.getPropertiesFileHelper().getAckTimeoutMS();
    }
}
