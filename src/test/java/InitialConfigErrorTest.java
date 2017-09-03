import com.splunk.cloudfwd.*;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import static com.splunk.cloudfwd.PropertyKeys.BLOCKING_TIMEOUT_MS;
import static com.splunk.cloudfwd.PropertyKeys.MOCK_HTTP_CLASSNAME;

/**
 * Test class to test for initial configuration errors that
 * should be detected before sending any events.
 *
 * Created by eprokop on 9/1/17.
 */
public class InitialConfigErrorTest extends AbstractConnectionTest {
    private int numEvents = 10;
    private enum ConfigError {
        ACKS_DISABLED,
        INVALID_TOKEN
    }
    private ConfigError errorToTest;

    protected int getNumEventsToSend() {
        return numEvents;
    }

    @Override
    protected BasicCallbacks getCallbacks() {
        return new BasicCallbacks(getNumEventsToSend()) {
            @Override
            public void failed(EventBatch events, Exception e) {
                Assert.assertTrue(e.getMessage(),
                        e instanceof HecErrorResponseException);
                System.out.println("Got expected exception: " + e);
                latch.countDown(); //allow the test to finish
            }

            @Override
            public void checkpoint(EventBatch events) {
                Assert.fail("We should fail before we checkpoint anything.");
            }

            @Override
            public void acknowledged(EventBatch events) {
                Assert.fail("We should fail before we get any acks.");
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
                        "com.splunk.cloudfwd.sim.errorgen.preflightfailure.AckDisabledEndpoints");
                break;
            case INVALID_TOKEN:
                props.put(MOCK_HTTP_CLASSNAME,
                        "com.splunk.cloudfwd.sim.errorgen.preflightfailure.InvalidTokenEndpoints");
                break;
            default:
                Assert.fail("Unsupported configuration error type");
        }
        props.put(BLOCKING_TIMEOUT_MS, "3000");
        return props;
    }

    // Need to separate this logic out of setUp() so that each Test
    // can use different simulated endpoints
    private void createConnection() {
        Properties props = new Properties();
        props.putAll(getTestProps());
        props.putAll(getProps());
        this.connection = new Connection((ConnectionCallbacks) callbacks, props);
        configureConnection(connection);
    }

    @Test
    public void sendWithAcksDisabled() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        errorToTest = ConfigError.ACKS_DISABLED;
        createConnection();
        try {
            super.sendEvents();
        } catch (HecConnectionTimeoutException e) {
            System.out.println(
                "Got expected timeout exception because all channels are unhealthy "
                + "due to acks disabled on token (per test design): "
                + e.getMessage());
        }
    }

    @Test
    public void sendToInvalidToken() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        errorToTest = ConfigError.INVALID_TOKEN;
        createConnection();
        try {
            super.sendEvents();
        } catch (HecConnectionTimeoutException e) {
            System.out.println(
                "Got expected timeout exception because all channels are unhealthy "
                + "due to invalid token (per test design): "
                + e.getMessage());
        }
    }
}
