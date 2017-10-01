import com.splunk.cloudfwd.error.HecAcknowledgmentTimeoutException;
import com.splunk.cloudfwd.error.HecConnectionStateException;
import com.splunk.cloudfwd.*;
import static com.splunk.cloudfwd.LifecycleEvent.Type.INDEXER_BUSY;
import static com.splunk.cloudfwd.error.HecConnectionStateException.Type.CONFIGURATION_EXCEPTION;
import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import com.splunk.cloudfwd.error.HecServerErrorResponseException;
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

    // Should be few enough events so that all make it through send() without blocking if preflight passes.
    // Otherwise, we might wait a long time to receive a blocking timeout since for some tests
    // channels are repeatedly flipped from healthy to unhealthy so the result is a slow
    // trickle of events through the system
    private int numEvents = 3;
    private enum Error {
        ACKS_DISABLED,
        INVALID_TOKEN,
        INDEXER_BUSY_POST,
        ACK_ID_DISABLED_AFTER_PREFLIGHT_SUCCEEDS,
        //when POST to /event endpoint not inside /event JSON envelope
        INVALID_EVENT_NUMBER //400 server reply: {"text":"Invalid data format","code":6,"invalid-event-number":0}
    }
    private Error errorToTest;

    protected int getNumEventsToSend() {
        if(!ackTimeoutLongerThanConnectionTimeout || errorToTest==Error.INVALID_EVENT_NUMBER){
            //In this case we are trying to generate an ack timeout.
            //due to timing we cannot guarantee that any messages except the first one sent would generate an ack timeout.
            //This is due to fact that the server response from the first message will (server busy/503/code:9) will mark
            //channel unhealthy and eventually HecConnectionTimeoutException will be seen. It's even less deterministic than
            //that because the async 503 response might come after the 1st message has been sent, or after the 20th, etc.
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
            public boolean shouldFail(){
                boolean shouldFail;
                switch(errorToTest) {
                    case ACK_ID_DISABLED_AFTER_PREFLIGHT_SUCCEEDS:
                    case INDEXER_BUSY_POST:
                    case ACKS_DISABLED:
                    case INVALID_TOKEN:
                    case INVALID_EVENT_NUMBER:
                        shouldFail = true;
                        break;
                    default:
                        shouldFail = false;
                }
                return shouldFail;
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
            protected boolean isExpectedFailureType(Exception e) {
                boolean isExpectedType;
                switch(errorToTest) {
                    case ACK_ID_DISABLED_AFTER_PREFLIGHT_SUCCEEDS:
                        isExpectedType = e instanceof HecConnectionStateException
                                && ((HecConnectionStateException) e).getType() == CONFIGURATION_EXCEPTION;
                        break;
                    case INDEXER_BUSY_POST:
                        if (ackTimeoutLongerThanConnectionTimeout) {
                            isExpectedType = e instanceof HecConnectionTimeoutException;
                        } else {
                            isExpectedType = e instanceof HecAcknowledgmentTimeoutException;
                        }
                        break;
                    case INVALID_TOKEN:
                        isExpectedType = e instanceof HecServerErrorResponseException
                                && ((HecServerErrorResponseException) e).getCode() == 4;
                        break;
                    case ACKS_DISABLED:
                        isExpectedType = e instanceof HecServerErrorResponseException
                            && ((HecServerErrorResponseException) e).getCode() == 14;
                        break;
                    case INVALID_EVENT_NUMBER:
                        if(e instanceof HecServerErrorResponseException) {
                            HecServerErrorResponseException srvrEx = (HecServerErrorResponseException) e;
                            Assert.assertEquals("Didn't find code 6", 6, srvrEx.getCode());
                            Assert.assertEquals("Didn't find invalid-event-number 0", 0, srvrEx.getInvalidEventNumber());
                            isExpectedType = true;
                        } else {
                            isExpectedType = false;
                        }
                        break;
                    default:
                        throw new RuntimeException("unhandled errToTest case");

//            protected boolean isFailureExpected(Exception e) {
//                if(errorToTest==Error.ACK_ID_DISABLED_AFTER_PREFLIGHT_SUCCEEDS){
//                    return e instanceof HecConnectionStateException
//                            && ((HecConnectionStateException)e).getType()==CONFIGURATION_EXCEPTION;
//                }else if (errorToTest == Error.INDEXER_BUSY_POST) {
//                    if (ackTimeoutLongerThanConnectionTimeout) {
//                        return e instanceof HecServerErrorResponseException;
//                    } else {
//                        return e instanceof HecAcknowledgmentTimeoutException;
//                    }
//                }else if(errorToTest==Error.INVALID_TOKEN){
//                    return e instanceof HecServerErrorResponseException;
//                }else if(errorToTest == Error.INVALID_EVENT_NUMBER){
//                    if(e instanceof HecServerErrorResponseException){
//                        HecServerErrorResponseException srvrEx = (HecServerErrorResponseException)e;
//                         Assert.assertEquals("Didn't find code 6", 6, srvrEx.getCode());
//                         Assert.assertEquals("Didn't find invalid-event-number 0", 0, srvrEx.getInvalidEventNumber());
//                        return true;
//                    }else{
//                        return false;
//                    }

                }
                    return isExpectedType;

            }
              
            @Override
            protected boolean isExpectedWarningType(Exception e){
                boolean isExpected;
                switch(errorToTest) {
                    case INDEXER_BUSY_POST:
                        isExpected = e instanceof HecServerErrorResponseException
                                && ((HecServerErrorResponseException)e).getLifecycleType()==INDEXER_BUSY;
                        break;
                    default:
                        isExpected = false;
                }
                return isExpected;

//            protected boolean isWarnExpected(Exception e){
//                return e instanceof HecServerErrorResponseException
//                        && ((HecServerErrorResponseException)e).getLifecycleType()==INDEXER_BUSY;

            }

            @Override
            public boolean shouldWarn(){
                boolean shouldWarn;
                switch(errorToTest) {
                    case INDEXER_BUSY_POST:
                        shouldWarn = true;
                        break;
                    case INVALID_TOKEN:
                    case ACKS_DISABLED:
                        shouldWarn = false;
                        break;
                    default:
                        shouldWarn = false;
                }
                return shouldWarn;
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
            case ACK_ID_DISABLED_AFTER_PREFLIGHT_SUCCEEDS: 
                //in this case, the pre-flight check will pass, and we are simulating were we detect acks disabled on event post
                props.put(MOCK_HTTP_CLASSNAME,
                        "com.splunk.cloudfwd.impl.sim.errorgen.unhealthy.EventPostNoAckIdEndpoints");
                break;
            case INVALID_EVENT_NUMBER:
                props.put(MOCK_HTTP_CLASSNAME,
                        "com.splunk.cloudfwd.impl.sim.errorgen.invalidvent.InvalidEventEndpoint");
                break;
            default:
                Assert.fail("Unsupported configuration error type");
        }

        if(ackTimeoutLongerThanConnectionTimeout){
            props.put(ACK_TIMEOUT_MS, "500000");  //in this case we excpect to see HecConnectionTimeoutException
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

    //pre-flight check NOT ok
    @Test
    public void sendWithAcksDisabled() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        LOG.info("TESTING ACKS_DISABLED");
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
    }

    //pre-flight check NOT ok
    @Test
    public void sendToInvalidToken() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        LOG.info("TESTING INVALID_TOKEN");
        errorToTest = Error.INVALID_TOKEN;
        ackTimeoutLongerThanConnectionTimeout = true;
        createConnection();
        super.sendEvents();
    }

    @Test
    public void postToBusyIndexerButHealthCheckOK() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
         LOG.info("TESTING INDEXER_BUSY_POST with HecConnectionTimeoutException expected");
        errorToTest = Error.INDEXER_BUSY_POST;
        ackTimeoutLongerThanConnectionTimeout = true;
        createConnection();
        super.sendEvents();
    }

    
    @Test
    public void postToBusyIndexerButHealthCheckOKAndExpectAckTimeout() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        LOG.info("TESTING INDEXER_BUSY_POST with HecAcknowledgementTimeoutException expected");
        errorToTest = Error.INDEXER_BUSY_POST;
        ackTimeoutLongerThanConnectionTimeout = false;
        createConnection();
        super.sendEvents();
        connection.closeNow(); //have to do this else we are going to get
    }    

    @Test
    public void postNoAckIdEvent() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        LOG.info("TESTING ACK_ID_DISABLED_AFTER_PREFLIGHT_SUCCEEDS");
        errorToTest = Error.ACK_ID_DISABLED_AFTER_PREFLIGHT_SUCCEEDS;
        ackTimeoutLongerThanConnectionTimeout = true;
        createConnection();
        super.sendEvents();
    }

    @Override
    protected boolean isExpectedSendException(Exception e) {
        boolean isExpected = false;
        switch (errorToTest) {
            case INVALID_TOKEN:
            case ACKS_DISABLED:
            case INVALID_EVENT_NUMBER:
                if (e instanceof HecConnectionTimeoutException) {
                    isExpected = true;
                }
                break;
            default:
                isExpected = false;
                break;
        }
        return isExpected;
    }

    @Override
    protected boolean shouldSendThrowException() {
        boolean shouldThrow;
        switch (errorToTest) {
            case ACKS_DISABLED:
            case INVALID_TOKEN:
                shouldThrow = true;
                break;
            case INDEXER_BUSY_POST:
            case ACK_ID_DISABLED_AFTER_PREFLIGHT_SUCCEEDS:
            case INVALID_EVENT_NUMBER:
                shouldThrow = false;
                break;
            default:
                shouldThrow = false;
                break;
        }
        return shouldThrow;
    }
    
    @Test
    public void postInvalidEvent() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        LOG.info("TESTING INVALID_EVENT_NUMBER");
        errorToTest = Error.INVALID_EVENT_NUMBER;
        ackTimeoutLongerThanConnectionTimeout = true;
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
    
      protected Event nextEvent(int seqno) {
          if(errorToTest == Error. INVALID_EVENT_NUMBER){
              //we need to intntionally generate data that does not have "/events" 'envelope'
              return super.getUnvalidatedBytesToRawEndpoint(seqno); //Generate BAD data
          }else{
              return super.nextEvent(seqno); //for all the other tests "normal" behvaior
          }
          
      }

}
