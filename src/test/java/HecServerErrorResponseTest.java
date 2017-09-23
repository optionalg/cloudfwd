import com.splunk.cloudfwd.*;
import com.splunk.cloudfwd.HecConnectionTimeoutException;
import com.splunk.cloudfwd.HecServerErrorResponseException;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import static com.splunk.cloudfwd.PropertyKeys.*;

/**
 * Test class to that tests various error rseponse scenarios
 * from HEC to ensure we are calling the correct callbacks.
 *
 * Created by eprokop on 9/1/17.
 */
public class HecServerErrorResponseTest extends AbstractConnectionTest {
    private static final Logger LOG = LoggerFactory.getLogger(HecServerErrorResponseTest.class.getName());
    private boolean ackTimeoutLongerThanConnectionTimeout;

    private int numEvents = 100;
    private enum Error {
        ACKS_DISABLED,
        INVALID_TOKEN,
        INDEXER_BUSY_POST,
        ACK_ID_DISABLED
    }
    private Error errorToTest;

    protected int getNumEventsToSend() {
        if(!ackTimeoutLongerThanConnectionTimeout){
            //In this case we are tring to generate an ack timeout.
            //due to timing we cannot guarnantee that any messages except the first one sent would generate an ack timeout.
            //This is due to fact that the server response from the first message will (server busy/503/code:9) will mark
            //channel unhealthy and eventually HecConnectionTimeoutException will be seen. It's even less deterministic than
            //that because the async 503 response might come after the 1st message has been sent, or afer the 20th, etc.
            //So all you can say is that at *some* point the channel will get marked unhealthy. So *some* number (1 or more)
            //of initially sent messages will ack timeout because they sneak in before the channel marked unhealthy. To make
            //it testable we just test 1 event, and have a different test that insures that HecConnectionSendException happens
            //at *some* point.
            return 1; 
        }else{
            return numEvents;
        }
    }

    @Override
    protected BasicCallbacks getCallbacks() {
        return new BasicCallbacks(getNumEventsToSend()) {
            @Override
            public void failed(EventBatch events, Exception e) {
                if(!ackTimeoutLongerThanConnectionTimeout){
                    LOG.trace("Got exception: " +  e);
                    Assert.assertTrue(e.getMessage(),
                            e instanceof HecAcknowledgmentTimeoutException);
                    LOG.trace("Got expected exception: " + e);
                }else{ //for bad tokens, etc that this test tests for
                    //FIXME TODO make this a little more specific by checking the code
                    LOG.trace("Got exception: " +  e);
                    Assert.assertTrue(e.getMessage(),
                            e instanceof HecServerErrorResponseException);
                    LOG.trace("Got expected exception: " + e);
                }
                super.failed(events, e);
            }

            @Override
            public void checkpoint(EventBatch events) {
                Assert.fail("We should fail before we checkpoint anything.");
            }

            @Override
            public void acknowledged(EventBatch events) {
                Assert.fail("We should fail before we get any acks.");
            }

            @Override
            protected boolean isFailureExpected(Exception e) {
                if(ackTimeoutLongerThanConnectionTimeout){
                    return e instanceof HecServerErrorResponseException;
                }else{
                    return e instanceof HecAcknowledgmentTimeoutException;
                }
            }
            
            @Override
              public boolean shouldFail(){
                return true;
             }

        };
    }

    @Override
    public void setUp() {
        this.callbacks = getCallbacks();
        this.testMethodGUID = java.util.UUID.randomUUID().toString();
        this.events = new ArrayList<>();
    }

    @Override
    protected Properties getProps() {
        Properties props = new Properties();
        switch(errorToTest) {
            case ACKS_DISABLED:
                props.put(MOCK_HTTP_CLASSNAME,
                        "com.splunk.cloudfwd.impl.sim.errorgen.splunkcheckfailure.AckDisabledEndpoints");
                break;
            case INVALID_TOKEN:
                props.put(MOCK_HTTP_CLASSNAME,
                        "com.splunk.cloudfwd.impl.sim.errorgen.splunkcheckfailure.InvalidTokenEndpoints");
                break;
            case INDEXER_BUSY_POST:
                props.put(MOCK_HTTP_CLASSNAME,
                        "com.splunk.cloudfwd.impl.sim.errorgen.unhealthy.EventPostIndexerBusyEndpoints");
                break;
            case ACK_ID_DISABLED:
                props.put(MOCK_HTTP_CLASSNAME,
                        "com.splunk.cloudfwd.impl.sim.errorgen.unhealthy.EventPostNoAckIdEndpoints");
                break;
            default:
                Assert.fail("Unsupported configuration error type");
        }

        if(ackTimeoutLongerThanConnectionTimeout){
            props.put(ACK_TIMEOUT_MS, "10000");  //in this case we excpect to see HecConnectionTimeoutException
        }else{
            props.put(ACK_TIMEOUT_MS, "2000");  //in this case we expect HecAcknowledgementTimeoutException
        }
        props.put(BLOCKING_TIMEOUT_MS, "5000"); 
        return props;
    }

    // Need to separate this logic out of setUp() so that each Test
    // can use different simulated endpoints
    private void createConnection() {
        Properties props = new Properties();
        props.putAll(getTestProps());
        props.putAll(getProps());
        this.connection = Connections.create((ConnectionCallbacks) callbacks, props);
        configureConnection(connection);
    }

    @Test
    public void sendWithAcksDisabled() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        errorToTest = Error.ACKS_DISABLED;
        ackTimeoutLongerThanConnectionTimeout = true;
        createConnection();
        try {
            super.sendEvents();
        } catch (HecConnectionTimeoutException e) {
            LOG.trace("Got expected timeout exception because all channels are unhealthy "
                + "due to acks disabled on token (per test design): "
                + e.getMessage());
        }
        Assert.assertTrue("Should receive a failed callback for acks disabled.", callbacks.isFailed());
        Assert.assertTrue("Exception should be an instance of HecServerErrorResponseException", callbacks.getException() instanceof HecServerErrorResponseException);
        HecServerErrorResponseException e = (HecServerErrorResponseException)(callbacks.getException());
        Assert.assertTrue("Exception code should be 14.", e.getCode() == 14);
    }

    @Test
    public void sendToInvalidToken() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        errorToTest = Error.INVALID_TOKEN;
        ackTimeoutLongerThanConnectionTimeout = true;
        createConnection();
        try {
            super.sendEvents();
        } catch (HecConnectionTimeoutException e) {
            LOG.trace("Got expected timeout exception because all channels are unhealthy "
                + "due to invalid token (per test design): "
                + e.getMessage());
        }
        Assert.assertTrue("Should receive a failed callback for invalid token.", callbacks.isFailed());
        Assert.assertTrue("Exception should be an instance of HecServerErrorResponseException", callbacks.getException() instanceof HecServerErrorResponseException);
        HecServerErrorResponseException e = (HecServerErrorResponseException)(callbacks.getException());
        Assert.assertTrue("Exception code should be 4.", e.getCode() == 4);
    }

    @Test
    public void postToBusyIndexerButHealthCheckOK() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        errorToTest = Error.INDEXER_BUSY_POST;
        ackTimeoutLongerThanConnectionTimeout = true;
        createConnection();
        try {
            super.sendEvents();
        } catch (HecConnectionTimeoutException e) {
            if(ackTimeoutLongerThanConnectionTimeout){                
                LOG.trace("Got expected timeout exception because all channels are unhealthy "
                        + "due to indexer being busy (per test design): "
                        + e.getMessage());            
                Assert.assertTrue("Got Expected HecConnectionTimeoutException", e instanceof HecConnectionTimeoutException);
            }else{
                Assert.fail("got Unknown exception when expecting failed callback for HecAcknowledgementTimeoutException: " + e);
            }
        }
    }
    
    @Test
    public void postToBusyIndexerButHealthCheckOKAndExcpectAckTimeout() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        errorToTest = Error.INDEXER_BUSY_POST;
        ackTimeoutLongerThanConnectionTimeout = false;
        createConnection();
        super.sendEvents();
        connection.closeNow(); //have to do this else we are going to get         
        Assert.assertTrue("didn't get failed callback with HecAcknowledgementTimeoutException", callbacks.getException() instanceof HecAcknowledgmentTimeoutException );
    }    

    @Test
    public void postNoAckIdEvent() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        errorToTest = Error.ACK_ID_DISABLED;
        createConnection();
        try {
            super.sendEvents();
        } catch (HecConnectionTimeoutException e) {
            LOG.trace("Got expected timeout exception because all channels are unhealthy "
                    + "due to indexer being busy (per test design): "
                    + e.getMessage());
        }
        // TODO: we are currently not calling any failed callbacks in this case. Do we want to?
    }
}
