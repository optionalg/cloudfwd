import com.splunk.cloudfwd.ConnectionCallbacks;
import com.splunk.cloudfwd.Connections;
import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import com.splunk.cloudfwd.error.HecMaxRetriesException;
import org.junit.Assert;
import org.junit.Test;

import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Properties;

/**
 * Created by mhora on 10/4/17.
 */
public class DownIndexersAllDownTest extends AbstractConnectionTest {

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
        props.put(PropertyKeys.MOCK_HTTP_CLASSNAME,
            "com.splunk.cloudfwd.impl.sim.errorgen.indexer.DownIndexerEndpoints");
        props.put(PropertyKeys.MAX_TOTAL_CHANNELS, "4");
        props.put(PropertyKeys.BLOCKING_TIMEOUT_MS, "10000");
        props.put(PropertyKeys.HEALTH_POLL_MS, "1000");
        props.put(PropertyKeys.ACK_TIMEOUT_MS, "60000");
        props.put(PropertyKeys.UNRESPONSIVE_MS, "-1"); //no dead channel detection

        return props;
    }

    // Need to separate this logic out of setUp() so that each Test
    // can use different simulated endpoints
    private void createConnection() {
        this.callbacks = getCallbacks();

        Properties props = new Properties();
        props.putAll(getTestProps());
        props.putAll(getProps());
        this.connection = Connections.create((ConnectionCallbacks) callbacks, props);
        configureConnection(connection);
    }

    @Override
    protected BasicCallbacks getCallbacks() {
        return new BasicCallbacks(getNumEventsToSend()) {
            @Override
            public boolean shouldFail() {
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
        };
    }

    @Override
    protected boolean isExpectedSendException(Exception e) {
        boolean isExpected = false;
        if (e instanceof HecConnectionTimeoutException) {
            isExpected = true;
        }
        return isExpected;
    }

    @Override
    protected boolean shouldSendThrowException() {
        boolean shouldThrow = true;
        //this.callbacks.latch.countDown(); // allow the test to finish
        return shouldThrow;
    }

    @Test
    public void sendToDownIndexers() throws InterruptedException {
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
}
