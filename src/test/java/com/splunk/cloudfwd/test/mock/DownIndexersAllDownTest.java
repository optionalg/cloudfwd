package com.splunk.cloudfwd.test.mock;

import com.splunk.cloudfwd.ConnectionCallbacks;
import com.splunk.cloudfwd.Connections;
import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import com.splunk.cloudfwd.error.HecMaxRetriesException;
import com.splunk.cloudfwd.test.util.AbstractConnectionTest;
import com.splunk.cloudfwd.test.util.BasicCallbacks;
import com.splunk.cloudfwd.impl.util.PropertiesFileHelper;
import org.junit.Assert;
import org.junit.Test;

import java.net.ConnectException;
import java.util.ArrayList;

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
    protected void setProps(PropertiesFileHelper settings) {
        settings.setMockHttpClassname("com.splunk.cloudfwd.impl.sim.errorgen.indexer.DownIndexerEndpoints");
        settings.setMaxTotalChannels(4);
        settings.setBlockingTimeoutMS(10000);
        settings.setHealthPollMS(1000);
        settings.setAckTimeoutMS(60000);
        settings.setUnresponsiveMS(-1); //no dead channel detection
    }

    // Need to separate this logic out of setUp() so that each Test
    // can use different simulated endpoints
    private void createConnection() {
        this.callbacks = getCallbacks();

        PropertiesFileHelper settings = getTestProps();
        setProps(settings);
        connection = Connections.create((ConnectionCallbacks) callbacks, settings);
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
            Assert.assertTrue("Expected HecMaxRetriesException, got " + e, e instanceof HecMaxRetriesException);
            gotException = true;
        }
        if(!gotException){
            Assert.fail("Expected HecMaxRetriedException associated with Connection instantiation config checks'");
        }
    }
}
