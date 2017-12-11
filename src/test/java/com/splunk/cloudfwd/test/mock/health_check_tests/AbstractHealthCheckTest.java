package com.splunk.cloudfwd.test.mock.health_check_tests;

import com.splunk.cloudfwd.ConnectionCallbacks;
import com.splunk.cloudfwd.Connections;
import com.splunk.cloudfwd.LifecycleEvent;
import com.splunk.cloudfwd.error.HecServerErrorResponseException;
import com.splunk.cloudfwd.impl.util.PropertiesFileHelper;
import org.junit.Assert;
import com.splunk.cloudfwd.test.util.AbstractConnectionTest;

import java.util.ArrayList;

/**
 * Created by mhora on 10/4/17.
 */
public class AbstractHealthCheckTest extends AbstractConnectionTest {
    private int numEvents = 10;

    protected int getNumEventsToSend() {
        return numEvents;
    }

    @Override
    public void setUp() {
        this.callbacks = getCallbacks();
        this.testMethodGUID = java.util.UUID.randomUUID().toString();
        this.events = new ArrayList<>();
    }

    // Need to separate this logic out of setUp() so that each Test
    // can use different simulated endpoints
    protected void createConnection(LifecycleEvent.Type problemType) {
        PropertiesFileHelper settings = this.getTestProps();
        setProps(settings);
        boolean gotException = false;
        try{
            connection = Connections.create((ConnectionCallbacks) callbacks, settings);
        }catch(Exception e){
            Assert.assertTrue("Expected HecServerErrorResponseException but got "+ e,  e instanceof HecServerErrorResponseException);
            HecServerErrorResponseException servRespExc = (HecServerErrorResponseException) e;
            Assert.assertTrue("HecServerErrorResponseException not "+problemType+", was  " + servRespExc.getLifecycleType(),
                    servRespExc.getLifecycleType()==problemType);
            gotException = true;
        }
        if(!gotException){
            Assert.fail("Expected HecMaxRetriedException associated with Connection instantiation config checks'");
        }
        configureConnection(connection);
    }
}
