package integration_tests;

import com.splunk.cloudfwd.LifecycleEvent;
import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.error.HecServerErrorResponseException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

/**
 * Created by eprokop on 10/4/17.
 */
public class CreateConnectionAcksDisabledIT extends AbstractReconciliationTest {
    @Override
    protected int getNumEventsToSend() {
        return 0;
    }

    @Test
    public void createConnectionWithAcksDisabled() {
    }

    @Override
    protected Properties getProps() {
        Properties p = super.getProps();
        p.setProperty(PropertyKeys.TOKEN, createTestToken(null, false));
        p.setProperty(PropertyKeys.MAX_TOTAL_CHANNELS, "1");
        return p;
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
