import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import com.splunk.cloudfwd.*;
import com.splunk.cloudfwd.error.HecMaxRetriesException;
import com.splunk.cloudfwd.error.HecServerErrorResponseException;
import com.splunk.cloudfwd.impl.sim.errorgen.indexer.RollingRestartEndpoints;

import org.junit.Assert;
import org.junit.Test;

import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Properties;

/**
 * Test class to gracefully handle different indexer states
 *
 * Created by meemax
 */
public class DownIndexersTest extends AbstractConnectionTest {
    private enum ClusterState {
        ALL_DOWN,
        ROLLING_RESTART
    }
    private ClusterState stateToTest;
    
    protected int getNumEventsToSend() {
        return 1000;
    }

    @Override
    public void setUp() {
        this.testMethodGUID = java.util.UUID.randomUUID().toString();
        this.events = new ArrayList<>();
    }

    @Override
    protected Properties getProps() {
        Properties props = new Properties();
        switch(this.stateToTest) {
            case ALL_DOWN:
                props.put(PropertyKeys.MOCK_HTTP_CLASSNAME,
                        "com.splunk.cloudfwd.impl.sim.errorgen.indexer.DownIndexerEndpoints");
                props.put(PropertyKeys.MAX_TOTAL_CHANNELS, "4");
                break;
           case ROLLING_RESTART:
              props.put(PropertyKeys.MOCK_HTTP_CLASSNAME,
                   "com.splunk.cloudfwd.impl.sim.errorgen.indexer.RollingRestartEndpoints");

              // mocking 4 indexers with 1 channel each
              // although no guarantee which channel goes to which indexer by LoadBalancer
              // but simulate anyway
              props.put(PropertyKeys.COLLECTOR_URI,
                  "https://127.0.0.1:8088,https://127.0.1.1:8088,https://127.0.2.1:8088,https://127.0.3.1:8088");
              props.put(PropertyKeys.MOCK_FORCE_URL_MAP_TO_ONE, "true");
              props.put(PropertyKeys.MAX_TOTAL_CHANNELS, "4");
              props.put(PropertyKeys.MAX_UNACKED_EVENT_BATCHES_PER_CHANNEL, "2");
              RollingRestartEndpoints.init(4, 1);
              break;
            default:
              Assert.fail("Unsupported configuration error type");
        }
        props.put(PropertyKeys.BLOCKING_TIMEOUT_MS, "10000");
        props.put(PropertyKeys.HEALTH_POLL_MS, "1000");
         props.put(PropertyKeys.ACK_TIMEOUT_MS, "60000");
        props.put(PropertyKeys.UNRESPONSIVE_MS, "-1"); //no dead channel detection

        return props;
    }

    // Need to separate this logic out of setUp() so that each Test
    // can use different simulated endpoints
    private void createConnection() {
        switch(stateToTest) {
            case ALL_DOWN:
                this.callbacks = new AllDownCallbacks(getNumEventsToSend());
                break;
            case ROLLING_RESTART:
                this.callbacks = getCallbacks();
                break;
            default:
                Assert.fail("Unsupported state to test");
        }

        Properties props = new Properties();
        props.putAll(getTestProps());
        props.putAll(getProps());
        this.connection = Connections.create((ConnectionCallbacks) callbacks, props);
        configureConnection(connection);
    }

    @Test
    public void sendToDownIndexers() throws InterruptedException {
        stateToTest = ClusterState.ALL_DOWN;
        boolean gotException = false;
        try{
            createConnection();
        }catch(Exception e){
            Assert.assertTrue("Expected HecMaxRetriesException, got " + e.getClass().getName(), e instanceof HecMaxRetriesException);
            gotException = true;
        }
        if(!gotException){
            Assert.fail("Expected HecMaxRetriedException associated with Connection instantiation config checks'");
        }
        
    }

    @Test
    public void sendToIndexersInRollingRestart() throws InterruptedException {
        stateToTest = ClusterState.ROLLING_RESTART;
        createConnection();
        super.sendEvents();
    }

    @Override
    protected boolean isExpectedSendException(Exception e) {
        boolean isExpected = false;
        switch (stateToTest) {
            case ALL_DOWN:
                if (e instanceof HecConnectionTimeoutException) {
                    isExpected = true;
                }
                break;
            case ROLLING_RESTART:
                isExpected = false;
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
        switch (stateToTest) {
            case ALL_DOWN:
                shouldThrow = true;
                break;
            case ROLLING_RESTART:
                shouldThrow = false;
                break;
            default:
                shouldThrow = false;
                break;
        }
        //this.callbacks.latch.countDown(); // allow the test to finish
        return shouldThrow;
    }

    private class AllDownCallbacks extends BasicCallbacks {

        public AllDownCallbacks(int expected) {
            super(expected);
        }

        @Override
        public boolean shouldFail(){
            return true;
        }

        @Override
        protected boolean isExpectedFailureType(Exception e) {
            boolean correctType = false;
            if (e instanceof ConnectException) { // TODO: make this exception more accurate to expected behavior
                correctType = true;
            }
            return correctType;
        }
    }
}