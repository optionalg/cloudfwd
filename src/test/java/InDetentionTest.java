import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import com.splunk.cloudfwd.error.HecServerErrorResponseException;
import com.splunk.cloudfwd.*;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Properties;

import static com.splunk.cloudfwd.PropertyKeys.BLOCKING_TIMEOUT_MS;
import static com.splunk.cloudfwd.PropertyKeys.MOCK_HTTP_CLASSNAME;

/**
 * Test class to gracefully handle different indexer states
 *
 * Created by meemax
 */
public class InDetentionTest extends AbstractConnectionTest {
    private int numEvents = 10;
    private enum ClusterState {
        ALL_IN_DETENTION,
        SOME_IN_DETENTION
    }
    private ClusterState stateToTest;
    
    
 @Override
    protected BasicCallbacks getCallbacks() {
        return new BasicCallbacks(getNumEventsToSend()) {

            @Override
            protected boolean isExpectedWarningType(Exception e) {
                return (e instanceof HecServerErrorResponseException &&
                        ((HecServerErrorResponseException)e).getLifecycleType()==LifecycleEvent.Type.INDEXER_IN_DETENTION);
            }
            
            @Override
              public boolean shouldWarn(){
                return true; //each failed preflight test will return INDEXER_IN_DETENTION via a systemWarning callback
             }

        };
    }    


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
        switch(stateToTest) {
            case ALL_IN_DETENTION:
                props.put(MOCK_HTTP_CLASSNAME,
                        "com.splunk.cloudfwd.impl.sim.errorgen.indexer.InDetentionEndpoints");
                break;
            case SOME_IN_DETENTION:
                props.put(MOCK_HTTP_CLASSNAME,
                        "com.splunk.cloudfwd.impl.sim.errorgen.indexer.SomeInDetentionEndpoints");
                break;
            default:
                Assert.fail("Unsupported configuration error type");
        }
        props.put(BLOCKING_TIMEOUT_MS, "3000");
        props.put(PropertyKeys.UNRESPONSIVE_MS, "-1"); //no dead channel detection
        props.put(PropertyKeys.MAX_TOTAL_CHANNELS, "4");
       
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
    }    

    /*
    @Test
    public void sendToIndexersInDetention() throws InterruptedException {
        stateToTest = ClusterState.ALL_IN_DETENTION;
        createConnection(LifecycleEvent.Type.INDEXER_IN_DETENTION);
    }
*/

    @Test
    public void sendToSomeIndexersInDetention() throws InterruptedException {
        stateToTest = ClusterState.SOME_IN_DETENTION;
        createConnection();
        super.sendEvents();
        //Thread.sleep(5000);
    }

    @Override
    protected boolean isExpectedSendException(Exception e) {
         return e instanceof HecConnectionTimeoutException;
    }

    @Override
    protected boolean shouldSendThrowException() { //fixme todo - it ain't even gonna get to send. It will fail fast instantiating connection
        return stateToTest == ClusterState.ALL_IN_DETENTION;
    }
}
