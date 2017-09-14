import com.splunk.cloudfwd.impl.EventBatchImpl;
import com.splunk.cloudfwd.impl.ConnectionImpl;
import com.splunk.cloudfwd.*;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import static com.splunk.cloudfwd.PropertyKeys.BLOCKING_TIMEOUT_MS;
import static com.splunk.cloudfwd.PropertyKeys.MOCK_HTTP_CLASSNAME;

/**
 * Test class to that tests various error rseponse scenarios
 * from HEC to ensure we are calling the correct callbacks.
 *
 * Created by eprokop on 9/1/17.
 */
public class HecErrorResponseTest extends AbstractConnectionTest {
    private static final Logger LOG = LoggerFactory.getLogger(HecErrorResponseTest.class.getName());

    private int numEvents = 10;
    private enum Error {
        ACKS_DISABLED,
        INVALID_TOKEN,
        INDEXER_BUSY_POST,
        ACK_ID_DISABLED
    }
    private Error errorToTest;

    protected int getNumEventsToSend() {
        return numEvents;
    }

    @Override
    protected BasicCallbacks getCallbacks() {
        return new BasicCallbacks(getNumEventsToSend()) {
            @Override
            public void failed(EventBatchImpl events, Exception e) {
                Assert.assertTrue(e.getMessage(),
                        e instanceof HecErrorResponseException);
                LOG.trace("Got expected exception: " + e);
                latch.countDown(); //allow the test to finish
            }

            @Override
            public void checkpoint(EventBatchImpl events) {
                Assert.fail("We should fail before we checkpoint anything.");
            }

            @Override
            public void acknowledged(EventBatchImpl events) {
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
                        "com.splunk.cloudfwd.impl.sim.errorgen.preflightfailure.AckDisabledEndpoints");
                break;
            case INVALID_TOKEN:
                props.put(MOCK_HTTP_CLASSNAME,
                        "com.splunk.cloudfwd.impl.sim.errorgen.preflightfailure.InvalidTokenEndpoints");
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
        props.put(BLOCKING_TIMEOUT_MS, "3000");
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
    public void sendWithAcksDisabled() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        errorToTest = Error.ACKS_DISABLED;
        createConnection();
        try {
            super.sendEvents();
        } catch (HecConnectionTimeoutException e) {
            LOG.trace("Got expected timeout exception because all channels are unhealthy "
                + "due to acks disabled on token (per test design): "
                + e.getMessage());
        }
    }

    @Test
    public void sendToInvalidToken() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        errorToTest = Error.INVALID_TOKEN;
        createConnection();
        try {
            super.sendEvents();
        } catch (HecConnectionTimeoutException e) {
            LOG.trace("Got expected timeout exception because all channels are unhealthy "
                + "due to invalid token (per test design): "
                + e.getMessage());
        }
    }

    @Test
    public void postToBusyIndexer() throws InterruptedException, TimeoutException, HecConnectionTimeoutException {
        errorToTest = Error.INDEXER_BUSY_POST;
        createConnection();
        try {
            super.sendEvents();
        } catch (HecConnectionTimeoutException e) {
            LOG.trace("Got expected timeout exception because all channels are unhealthy "
                    + "due to indexer being busy (per test design): "
                    + e.getMessage());
        }
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
    }
}
