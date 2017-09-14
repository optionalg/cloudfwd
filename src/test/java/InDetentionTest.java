import com.splunk.cloudfwd.impl.ConnectionImpl;
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
        this.connection = new ConnectionImpl((ConnectionCallbacks) callbacks, props);
        configureConnection(connection);
    }

    @Test
    public void sendToIndexersInDetention() throws InterruptedException {
        stateToTest = ClusterState.ALL_IN_DETENTION;
        createConnection();
        try {
            super.sendEvents();
        } catch (HecConnectionTimeoutException e) {
            System.out.println(
                "Got expected timeout exception because all indexers are in detention "
                + e.getMessage());
            // allow test to pass
            super.callbacks.latch.countDown();
        }
    }

    @Test
    public void sendToSomeIndexersInDetention() throws InterruptedException {
        stateToTest = ClusterState.SOME_IN_DETENTION;
        createConnection();
        try {
            super.sendEvents();
            //super.callbacks.latch.countDown();
        } catch (HecConnectionTimeoutException e) {
            Assert.fail("Events should have been sent.");
        }
    }
}
