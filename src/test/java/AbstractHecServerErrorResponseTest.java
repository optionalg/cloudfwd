import com.splunk.cloudfwd.*;
import com.splunk.cloudfwd.error.HecServerErrorResponseException;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Properties;

/**
 * Created by mhora on 10/3/17.
 */
public abstract class AbstractHecServerErrorResponseTest extends AbstractConnectionTest {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractHecServerErrorResponseTest.class.getName());

    @Override
    protected BasicCallbacks getCallbacks() {
        return new BasicCallbacks(getNumEventsToSend()) {
            @Override
            public void checkpoint(EventBatch events) {
                Assert.fail("We should fail before we checkpoint anything.");
            }

            @Override
            public void acknowledged(EventBatch events) {
                Assert.fail("We should fail before we get any acks.");
            }

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

        };
    }

    @Override
    public void setUp() {
        this.callbacks = getCallbacks();
        this.testMethodGUID = java.util.UUID.randomUUID().toString();
        this.events = new ArrayList<>();
    }

    // Need to separate this logic out of setUp() so that each Test
    // can use different simulated endpoints
    protected void createConnection() {
        Properties props = new Properties();
        props.putAll(getTestProps());
        props.putAll(getProps());
        this.connection = Connections.create((ConnectionCallbacks) callbacks, props);
        configureConnection(connection);
    }

    protected void createConnection(LifecycleEvent.Type problemType) {
        Properties props = new Properties();
        props.putAll(getTestProps());
        props.putAll(getProps());
        boolean gotException = false;
        try{
            this.connection = Connections.create((ConnectionCallbacks) callbacks, props);
        }catch(Exception e){
            Assert.assertTrue("Expected HecServerErrorResponseException",  e instanceof HecServerErrorResponseException);
            HecServerErrorResponseException servRespExc = (HecServerErrorResponseException) e;
            Assert.assertTrue("HecServerErrorResponseException not "+problemType+", was  " + servRespExc.getLifecycleType(),
                    servRespExc.getLifecycleType()==problemType);
            gotException = true;
        }
        if(!gotException){
            Assert.fail("Expected HecMaxRetriedException associated with Connection instantiation config checks'");
        }
    }

}
