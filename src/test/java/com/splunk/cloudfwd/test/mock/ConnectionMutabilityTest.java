package com.splunk.cloudfwd.test.mock;

import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.ConnectionSettings;
import com.splunk.cloudfwd.Event;
import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import com.splunk.cloudfwd.impl.ConnectionImpl;
import com.splunk.cloudfwd.impl.sim.ValidatePropsEndpoint;
import com.splunk.cloudfwd.test.util.AbstractConnectionTest;
import org.junit.Assert;
import org.junit.Test;

import java.net.UnknownHostException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Created by eprokop on 9/11/17.
 */
public class ConnectionMutabilityTest extends AbstractConnectionTest {
    private int numEvents = 100000;
    private int start = 0;
    private int stop = -1;
    private long ackPollWait = 1000;

    @Test
    public void setMultipleProperties() throws Throwable {
        LOG.info("test:  setMultipleProperties");       
        setPropsOnEndpoint();
        connection.getSettings().setHecEndpointType(Connection.HecEndpoint.RAW_EVENTS_ENDPOINT);
        super.eventType = Event.Type.TEXT;
        LOG.info("sending first batch of events");
        sendSomeEvents(getNumEventsToSend()/4);

        // Set some new properties
        ConnectionSettings settings = connection.getSettings();
        settings.setToken("a token");
        settings.setAckTimeoutMS(120000);
        settings.setUrls("https://127.0.0.1:8188");
        setPropsOnEndpoint();
        LOG.info("sending second batch of events");
        sendSomeEvents(getNumEventsToSend()/4);


        // Set the same properties
        settings.setToken("a token");
        settings.setAckTimeoutMS(120000);
        settings.setUrls("https://127.0.0.1:8188");
        setPropsOnEndpoint();
        LOG.info("sending third batch of events");
        sendSomeEvents(getNumEventsToSend()/4);


        // Set some more new properties
        settings.setToken("different token");
        settings.setAckTimeoutMS(240000);
        settings.setUrls("https://127.0.0.1:8288, https://127.0.0.1:8388");
        setPropsOnEndpoint();
         LOG.info("sending fourth batch of events");
        sendSomeEvents(getNumEventsToSend()/4);
        close();
        checkAsserts();
    }

    
    @Test
    public void changeEndpointType() throws Throwable {
        LOG.info("test:  changeEndpointType");
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
        LOG.info("test:  changeToken");
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
        LOG.info("test:  changeUrlsAndAckTimeout");
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
    protected void configureProps(ConnectionSettings settings) {
        settings.setAckTimeoutMS(1000000);
        settings.setUnresponsiveMS(-1); //no dead channel detection
        settings.setMockHttp(true);
        // the asserts for this test exist in the endpoint since we must check values server side
        settings.setMockHttpClassname("com.splunk.cloudfwd.impl.sim.ValidatePropsEndpoint");
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

    private void setUrls(String urls) throws UnknownHostException {
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
        LOG.trace(
                "sendSomeEvents: SENDING EVENTS WITH CLASS GUID: " + AbstractConnectionTest.TEST_CLASS_INSTANCE_GUID
                        + "And test method GUID " + testMethodGUID);

        stop += numEvents;
        LOG.trace("sendSomeEvents: Start = "+start + " stop = " + stop);
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

    @Test
    public void setMultiplePropertiesDeprecated() throws Throwable {
        LOG.info("test:  setMultipleProperties");
        setPropsOnEndpoint();
        connection.getSettings().setHecEndpointType(Connection.HecEndpoint.RAW_EVENTS_ENDPOINT);
        super.eventType = Event.Type.TEXT;
        LOG.info("sending first batch of events");
        sendSomeEvents(getNumEventsToSend()/4);

        // Set some new properties
        Properties props1 = new Properties();
        props1.setProperty(PropertyKeys.TOKEN, "a token");
        props1.setProperty(PropertyKeys.ACK_TIMEOUT_MS, "120000");
        props1.setProperty(PropertyKeys.COLLECTOR_URI, "https://127.0.0.1:8188");
        connection.getSettings().setProperties(props1);
        setPropsOnEndpoint();
        LOG.info("sending second batch of events");
        sendSomeEvents(getNumEventsToSend()/4);


        // Set the same properties
//        connection.getSettings().setProperties(props1);
//        setPropsOnEndpoint();
//        LOG.info("sending third batch of events");
//        sendSomeEvents(getNumEventsToSend()/4);


        // Set some more new properties
//        Properties props2 = new Properties();
//        props2.setProperty(PropertyKeys.TOKEN, "different token");
//        props2.setProperty(PropertyKeys.ACK_TIMEOUT_MS, "240000");
//        props2.setProperty(PropertyKeys.COLLECTOR_URI, "https://127.0.0.1:8288, https://127.0.0.1:8388");
//        connection.getSettings().setProperties(props2);
//        setPropsOnEndpoint();
//        LOG.info("sending fourth batch of events");
//        sendSomeEvents(getNumEventsToSend()/4);
        close();
        checkAsserts();
    }
}
