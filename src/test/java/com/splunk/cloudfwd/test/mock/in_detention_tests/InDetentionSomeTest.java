package com.splunk.cloudfwd.test.mock.in_detention_tests;

import com.splunk.cloudfwd.test.util.BasicCallbacks;
import com.splunk.cloudfwd.impl.util.PropertiesFileHelper;
import com.splunk.cloudfwd.*;
import com.splunk.cloudfwd.error.HecServerErrorResponseException;
import org.junit.Test;

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
    protected void setProps(PropertiesFileHelper settings) {
        settings.setMockHttpClassname("com.splunk.cloudfwd.impl.sim.errorgen.indexer.SomeInDetentionEndpoints");
        settings.setBlockingTimeoutMS(30000);
        settings.setUnresponsiveMS(-1); //no dead channel detection
        settings.setMaxTotalChannels(2);
    }

    @Override
    protected boolean shouldSendThrowException() { //fixme todo - it ain't even gonna get to send. It will fail fast instantiating connection
        return false;
    }

    // Need to separate this logic out of setUp() so that each Test
    // can use different simulated endpoints
    protected void createConnection() {
        PropertiesFileHelper settings = this.getTestProps();
        this.setProps(settings);
        this.connection = Connections.create((ConnectionCallbacks) callbacks, settings);
        configureConnection(connection);
    }

    @Test
    public void sendToSomeIndexersInDetention() throws InterruptedException {
        createConnection();
        super.sendEvents();
        //Thread.sleep(5000);
    }
}
