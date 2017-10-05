package mock_tests;

import com.splunk.cloudfwd.PropertyKeys;
import test_utils.BasicCallbacks;
import com.splunk.cloudfwd.*;
import com.splunk.cloudfwd.error.HecServerErrorResponseException;
import org.junit.Test;
import java.util.Properties;

import static com.splunk.cloudfwd.PropertyKeys.BLOCKING_TIMEOUT_MS;
import static com.splunk.cloudfwd.PropertyKeys.MOCK_HTTP_CLASSNAME;

/**
 * Created by mhora on 10/4/17.
 */
public class InDetentionSomeTest extends AbstractInDetentionTest {

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
                return false; //each failed preflight test will return INDEXER_IN_DETENTION via a systemWarning callback
            }

        };
    }

    @Override
    protected Properties getProps() {
        Properties props = new Properties();
        props.put(MOCK_HTTP_CLASSNAME,
            "com.splunk.cloudfwd.impl.sim.errorgen.indexer.SomeInDetentionEndpoints");
        props.put(BLOCKING_TIMEOUT_MS, "30000");
        props.put(PropertyKeys.UNRESPONSIVE_MS, "-1"); //no dead channel detection
        props.put(PropertyKeys.MAX_TOTAL_CHANNELS, "2");

        return props;
    }

    @Override
    protected boolean shouldSendThrowException() { //fixme todo - it ain't even gonna get to send. It will fail fast instantiating connection
        return false;
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

    @Test
    public void sendToSomeIndexersInDetention() throws InterruptedException {
        createConnection();
        super.sendEvents();
        //Thread.sleep(5000);
    }
}
