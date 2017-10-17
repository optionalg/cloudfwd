package com.splunk.cloudfwd.test.mock;

import com.splunk.cloudfwd.ConnectionCallbacks;
import com.splunk.cloudfwd.Connections;
import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.impl.sim.errorgen.indexer.RollingRestartEndpoints;
import com.splunk.cloudfwd.test.util.AbstractConnectionTest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Properties;

/**
 * Created by mhora on 10/4/17.
 */
public class DownIndexersRollingRestartTest extends AbstractConnectionTest {
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
    protected boolean isExpectedSendException(Exception e) {
        return false;
    }

    @Override
    protected boolean shouldSendThrowException() {
        //this.callbacks.latch.countDown(); // allow the test to finish
        return false;
    }

    @Test
    public void sendToIndexersInRollingRestart() throws InterruptedException {
        createConnection();
        super.sendEvents();
    }


}
