import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.Event;
import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.impl.ConnectionImpl;
import com.splunk.cloudfwd.impl.sim.ValidatePropsEndpoint;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Created by eprokop on 9/11/17.
 */
public class ConnectionMutabilityTest extends AbstractConnectionTest {
    private int numEvents = 1000000;
    private int start = 0;
    private int stop = -1;
    private long ackPollWait = 1000;

    @Test
    // Makes sure we are computing diffs as expected
    public void testPropertiesDiffs() {
        Properties props1 = new Properties();
        props1.setProperty(PropertyKeys.TOKEN, "a token");
        props1.setProperty(PropertyKeys.ACK_TIMEOUT_MS, "30000");
        props1.setProperty(PropertyKeys.COLLECTOR_URI, "https://127.0.0.1:8088");
        connection.getSettings().setProperties(props1);

        // Diff for the same properties
        Properties props2 = new Properties();
        props2.setProperty(PropertyKeys.COLLECTOR_URI, "https://127.0.0.1:8088");
        props2.setProperty(PropertyKeys.ACK_TIMEOUT_MS, "30000");
        props2.setProperty(PropertyKeys.TOKEN, "a token");
        Properties diff = connection.getSettings().getDiff(props2);
        Assert.assertTrue("Diff should be empty.", diff.isEmpty());

        // Diff for some different properties
        Properties props3 = new Properties();
        String diffToken = "a different token";
        String diffUrl = "https://127.0.0.1:8188, https://127.0.0.1:8288, https://127.0.0.1:8388";
        String diffChannelDecom = "50000";
        props3.setProperty(PropertyKeys.ACK_TIMEOUT_MS, "30000"); // same as before
        props3.setProperty(PropertyKeys.TOKEN, diffToken);
        props3.setProperty(PropertyKeys.COLLECTOR_URI, diffUrl);
        props3.setProperty(PropertyKeys.CHANNEL_DECOM_MS, diffChannelDecom);
        diff = connection.getSettings().getDiff(props3);
        Assert.assertTrue("Diff should contain token.", diff.getProperty(PropertyKeys.TOKEN).equals(diffToken));
        Assert.assertTrue("Diff should contain urls.", diff.getProperty(PropertyKeys.COLLECTOR_URI).equals(diffUrl));
        Assert.assertTrue("Diff should contain channel decom time.", diff.getProperty(PropertyKeys.CHANNEL_DECOM_MS).equals(diffChannelDecom));
        Assert.assertTrue("Diff should contain exactly 3 elements.", diff.size() == 3);

        // Diff for empty Properties
        diff = connection.getSettings().getDiff(new Properties());
        Assert.assertTrue("Diff for empty properties should be empty.", diff.isEmpty());
    }

    @Test
    public void setMultipleProperties() throws Throwable {
        setPropsOnEndpoint();
        connection.getSettings().setHecEndpointType(Connection.HecEndpoint.RAW_EVENTS_ENDPOINT);
        super.eventType = Event.Type.TEXT;
        sendSomeEvents(getNumEventsToSend()/4);

        // Set some new properties
        Properties props1 = new Properties();
        props1.setProperty(PropertyKeys.TOKEN, "a token");
        props1.setProperty(PropertyKeys.ACK_TIMEOUT_MS, "120000");
        props1.setProperty(PropertyKeys.COLLECTOR_URI, "https://127.0.0.1:8188");
        connection.getSettings().setProperties(props1);
        setPropsOnEndpoint();
        sendSomeEvents(getNumEventsToSend()/4);


        // Set the same properties
        connection.getSettings().setProperties(props1);
        setPropsOnEndpoint();
        sendSomeEvents(getNumEventsToSend()/4);


        // Set some more new properties
        Properties props2 = new Properties();
        props2.setProperty(PropertyKeys.TOKEN, "different token");
        props2.setProperty(PropertyKeys.ACK_TIMEOUT_MS, "240000");
        props2.setProperty(PropertyKeys.COLLECTOR_URI, "https://127.0.0.1:8288, https://127.0.0.1:8388");
        connection.getSettings().setProperties(props2);
        setPropsOnEndpoint();
        sendSomeEvents(getNumEventsToSend()/4);
        close();
        checkAsserts();
    }

    @Test
    public void changeEndpointType() throws Throwable {
        setPropsOnEndpoint();
        connection.getSettings().setHecEndpointType(Connection.HecEndpoint.RAW_EVENTS_ENDPOINT);
        super.eventType = Event.Type.TEXT;
        sendSomeEvents(getNumEventsToSend()/4);
        

        connection.getSettings().setHecEndpointType(Connection.HecEndpoint.STRUCTURED_EVENTS_ENDPOINT);
        super.eventType = Event.Type.TEXT;
        sendSomeEvents(getNumEventsToSend()/4);
        

        connection.getSettings().setHecEndpointType(Connection.HecEndpoint.RAW_EVENTS_ENDPOINT);
        super.eventType = Event.Type.UNKNOWN;
        sendSomeEvents(getNumEventsToSend()/4);
        

        connection.getSettings().setHecEndpointType(Connection.HecEndpoint.STRUCTURED_EVENTS_ENDPOINT);
        super.eventType = Event.Type.UNKNOWN;
        sendSomeEvents(getNumEventsToSend()/4);

        close();
        checkAsserts();
    }

    @Test
    public void changeToken() throws Throwable {
        setPropsOnEndpoint();
        connection.getSettings().setHecEndpointType(Connection.HecEndpoint.RAW_EVENTS_ENDPOINT);
        super.eventType = Event.Type.TEXT;
        sendSomeEvents(getNumEventsToSend()/2);


        connection.getSettings().setToken("different token");
        setPropsOnEndpoint();
        sendSomeEvents(getNumEventsToSend()/2);
        close();
        checkAsserts();
    }

    @Test
    public void changeUrlsAndAckTimeout() throws Throwable {
        connection.getSettings().setHecEndpointType(Connection.HecEndpoint.RAW_EVENTS_ENDPOINT);
        super.eventType = Event.Type.TEXT;
        setPropsOnEndpoint();
        sendSomeEvents(getNumEventsToSend()/4);

        setUrls("https://127.0.0.1:8188");
        setAckTimeout(120000);

        sendSomeEvents(getNumEventsToSend()/4);

        setAckTimeout(65000);
        setUrls("https://127.0.0.1:8288, https://127.0.0.1:8388");

        sendSomeEvents(getNumEventsToSend()/4);

        setUrls("https://127.0.0.1:8488, https://127.0.0.1:8588, https://127.0.0.1:8688");
        setAckTimeout(80000);

        sendSomeEvents(getNumEventsToSend()/4);
        close();
        checkAsserts();
    }

    @Override
    protected Properties getProps() {
        Properties props = new Properties();
        props.put(PropertyKeys.ACK_TIMEOUT_MS, "1000000"); //we don't want the ack timout kicking in
        props.put(PropertyKeys.UNRESPONSIVE_MS, "-1"); //no dead channel detection
        props.put(PropertyKeys.MOCK_HTTP_KEY, "true");
        // the asserts for this test exist in the endpoint since we must check values server side
        props.put(PropertyKeys.MOCK_HTTP_CLASSNAME, "com.splunk.cloudfwd.impl.sim.ValidatePropsEndpoint");
        return props;
    }

    private void setPropsOnEndpoint() {
        ValidatePropsEndpoint.URLS = connection.getSettings().getUrls();
        ValidatePropsEndpoint.ACK_TIMEOUT_MS = connection.getSettings().getAckTimeoutMS();
        ValidatePropsEndpoint.TOKEN = connection.getSettings().getToken();
    }

    private void checkAsserts() throws Throwable {
        Throwable e;
        if ((e = ValidatePropsEndpoint.getAssertionFailures()) != null) {
            throw e;
        }
    }

    protected int getNumEventsToSend() {
        return numEvents;
    }

    private void setUrls(String urls) {
        connection.getSettings().setUrls(urls);
        ValidatePropsEndpoint.URLS = connection.getSettings().getUrls();
    }

    private void setAckTimeout(long ms) {
        connection.getSettings().setAckTimeoutMS(ms);
        ValidatePropsEndpoint.ACK_TIMEOUT_MS = connection.getSettings().getAckTimeoutMS();
    }

    @Override
    protected void configureConnection(Connection connection) {
        connection.getSettings().setEventBatchSize(1024*32); //32k batching batching, roughly
    }

    private void sendSomeEvents(int numEvents) throws InterruptedException, HecConnectionTimeoutException {
        System.out.println(
                "SENDING EVENTS WITH CLASS GUID: " + TEST_CLASS_INSTANCE_GUID
                        + "And test method GUID " + testMethodGUID);

        stop += numEvents;
        System.out.println("Start = "+start + " stop = " + stop);
        for (int i = start; i <= stop; i++) {
            Event event = nextEvent(i + 1);
            connection.send(event);
        }
        start = stop + 1;
        connection.flush();
        //this should really be done with a latch on acn ack counter in the acknowledged callback
        //but what the hell, test a different code path
        while(!((ConnectionImpl)connection).getUnackedEvents().isEmpty()){
            sleep(ackPollWait);
        }        
    }

    private void close() throws InterruptedException, HecConnectionTimeoutException {
        connection.close(); //will flush
        this.callbacks.await(10, TimeUnit.MINUTES);
        if (callbacks.isFailed()) {
            Assert.fail("There was a failure callback with exception class  "
                    + callbacks.getException() + " and message " + callbacks.getFailMsg());
        }
    }

    private void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }
}
