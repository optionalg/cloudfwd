import com.splunk.cloudfwd.*;
import com.splunk.cloudfwd.exceptions.HecMissingPropertiesException;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.splunk.cloudfwd.PropertyKeys.*;

/**
 * Created by mhora on 9/11/17.
 */
public class PropertiesConfigurationTest extends AbstractConnectionTest {
    private static final Logger LOG = LoggerFactory.getLogger(PropertiesConfigurationTest.class.getName());

    private int numEvents = 10;

    protected int getNumEventsToSend() {
        return numEvents;
    }

    @Override
    protected BasicCallbacks getCallbacks() {
        return new BasicCallbacks(getNumEventsToSend()) {
            @Override
            public void failed(EventBatch events, Exception e) {
                Assert.assertTrue(e.getMessage(),
                        e instanceof HecMissingPropertiesException);
                LOG.trace("Got expected exception: " + e);
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

    // PropertiesHelper Configuration Tests
    @Test
    public void testPropertiesHelperWithOverrides() throws MalformedURLException {
        // Need connection object to pass into PropertiesFileHelper constructor for failed() callback
        Properties overrides = new Properties();
        overrides.put(TOKEN, "foo-token");
        overrides.put(COLLECTOR_URI, "https://inputs1.kinesis1.foo.com:8088");
        overrides.put(EVENT_BATCH_SIZE, "100");
        List<URL> urls = new ArrayList<>();
        urls.add(new URL("https://inputs1.kinesis1.foo.com:8088"));

        this.connection = new Connection(callbacks, overrides);

        Assert.assertEquals(100, this.connection.getPropertiesFileHelper().getEventBatchSize()); // Override took effect
        Assert.assertEquals(Long.parseLong(DEFAULT_DECOM_MS), this.connection.getPropertiesFileHelper().getChannelDecomMS()); // No override or lb.properties value - use default
        Assert.assertEquals(urls, this.connection.getPropertiesFileHelper().getUrls()); //Override took effect
        Assert.assertEquals("foo-token", this.connection.getPropertiesFileHelper().getToken()); //Override took effect
        Assert.assertEquals(5000, this.connection.getPropertiesFileHelper().getAckPollMS()); // No override but use lb.properties value
    }

    @Test
    public void testPropertiesHelperWithoutOverrides() throws MalformedURLException {
        // Need connection object to pass into PropertiesFileHelper constructor for failed() callback
        Properties overrides = new Properties();

        this.connection = new Connection(callbacks, overrides);

        Assert.assertEquals(1000000, this.connection.getPropertiesFileHelper().getEventBatchSize()); // Property is in lb.properties
        Assert.assertEquals(Long.parseLong(DEFAULT_DECOM_MS), this.connection.getPropertiesFileHelper().getChannelDecomMS()); // Property is not in lb.properties so use default
    }
}
