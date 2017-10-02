import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import com.splunk.cloudfwd.*;
import static com.splunk.cloudfwd.LifecycleEvent.Type.ACK_DISABLED;
import static com.splunk.cloudfwd.LifecycleEvent.Type.INVALID_TOKEN;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import static com.splunk.cloudfwd.PropertyKeys.*;
import static com.splunk.cloudfwd.LifecycleEvent.Type.INVALID_AUTH;
import com.splunk.cloudfwd.error.HecServerErrorResponseException;

/**
 * Test class to that tests various error rseponse scenarios
 * from HEC to ensure we are calling the correct callbacks.
 *
 * Created by eprokop on 9/1/17.
 */
public class HealthCheckTest extends AbstractConnectionTest {
    private static final Logger LOG = LoggerFactory.getLogger(HealthCheckTest.class.getName());

    private int numEvents = 10;
    private enum Error {
        ACKS_DISABLED,
        INVALID_TOKEN,
        INVALID_AUTH,
        IN_DETENTION
    }
    private Error errorToTest;

    protected int getNumEventsToSend() {
        return numEvents;
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
            case INVALID_AUTH:
              props.put(MOCK_HTTP_CLASSNAME,
                      "com.splunk.cloudfwd.impl.sim.errorgen.splunkcheckfailure.InvalidAuthEndpoints");
              break;
            case IN_DETENTION:
              props.put(MOCK_HTTP_CLASSNAME,
                      "com.splunk.cloudfwd.impl.sim.errorgen.indexer.InDetentionEndpoints");
              break;
            default:
                Assert.fail("Unsupported configuration error type");
        }
        props.put(BLOCKING_TIMEOUT_MS, "3000");
        return props;
    }

    // Need to separate this logic out of setUp() so that each Test
    // can use different simulated endpoints
    private void createConnection(LifecycleEvent.Type problemType) {
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
        configureConnection(connection);
    }

    @Test
    public void checkAcksDisabled() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        errorToTest = Error.ACKS_DISABLED;
        createConnection(ACK_DISABLED);
    }

    @Test
    public void checkInvalidToken() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        errorToTest = Error.INVALID_TOKEN;
        createConnection(INVALID_TOKEN);
    }

    @Test
    public void checkInvalidAuth() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        errorToTest = Error.INVALID_AUTH;
        createConnection(INVALID_AUTH);
    }

    @Test
    public void checkInDetention() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        errorToTest = Error.IN_DETENTION;
        createConnection(LifecycleEvent.Type.INDEXER_IN_DETENTION);
    }
}
