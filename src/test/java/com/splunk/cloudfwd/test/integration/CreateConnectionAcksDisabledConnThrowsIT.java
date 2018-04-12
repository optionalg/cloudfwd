package com.splunk.cloudfwd.test.integration;

import com.splunk.cloudfwd.ConnectionSettings;
import com.splunk.cloudfwd.LifecycleEvent;
import com.splunk.cloudfwd.error.HecServerErrorResponseException;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by eprokop on 10/4/17.
 */
public class CreateConnectionAcksDisabledConnThrowsIT extends AbstractReconciliationTest {
    @Override
    protected int getNumEventsToSend() {
        return 0;
    }

    @Test
    public void createConnectionWithAcksDisabled() {
        
    }

    @Override
    protected void configureProps(ConnectionSettings settings) {
        super.configureProps(settings);
        settings.setConntctionThrowsExceptionOnCreation(true);
        settings.setToken(createTestToken(null, false));
        settings.setMaxTotalChannels(1);
    }

    @Override
    protected boolean connectionInstantiationShouldFail() {
        return true;
    }

    @Override
    protected boolean isExpectedConnInstantiationException(Exception e) {
        Assert.assertTrue("Exception should be the correct type.", e instanceof HecServerErrorResponseException);
        HecServerErrorResponseException ex = (HecServerErrorResponseException)e;
        Assert.assertEquals("Exception should have correct lifecycle type.", LifecycleEvent.Type.ACK_DISABLED, ex.getLifecycleType());
        Assert.assertEquals("Exception should have correct HecServerErrorResponseException type.", HecServerErrorResponseException.Type.RECOVERABLE_CONFIG_ERROR, ex.getErrorType());
        return true;
    }
}
