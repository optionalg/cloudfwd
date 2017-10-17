package com.splunk.cloudfwd.test.mock.in_detention_tests;

import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import com.splunk.cloudfwd.test.util.AbstractConnectionTest;

import java.util.ArrayList;

/**
 * Created by mhora on 10/4/17.
 */
public abstract class AbstractInDetentionTest extends AbstractConnectionTest {
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

    @Override
    protected boolean isExpectedSendException(Exception e) {
        return e instanceof HecConnectionTimeoutException;
    }


}
