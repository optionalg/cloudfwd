import com.splunk.cloudfwd.Connections;
import com.splunk.cloudfwd.LifecycleEvent;
import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.error.HecServerErrorResponseException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

/**
 * Created by eprokop on 10/5/17.
 */
public class CreateConnectionInvalidTokenIT extends AbstractReconciliationTest {

    // Scenario: Connection created with invalid token
    // Expected behavior: Connection fails to instantiate and throws proper exception
    @Test
    public void createConnectionWithInvalidToken() {
    }

    @Override
    protected Properties getProps() {
        Properties p = super.getProps();
        p.put(PropertyKeys.TOKEN, "invalid_token");
        return p;
    }

    @Override
    protected int getNumEventsToSend() {
        return 0;
    }

    @Override
    protected boolean connectionInstantiationShouldFail() {
        return true;
    }

    @Override
    protected boolean isExpectedConnInstantiationException(Exception e) {
        Assert.assertTrue("Exception should be the correct type.", e instanceof HecServerErrorResponseException);
        HecServerErrorResponseException ex = (HecServerErrorResponseException)e;
        Assert.assertNotNull("Exception should be thrown when creating connection with invalid token on all channels.", ex);
        Assert.assertEquals("Exception should have correct lifecycle type.", LifecycleEvent.Type.INVALID_TOKEN, ex.getLifecycleType());
        Assert.assertEquals("Exception should have correct HecServerErrorResponseException type.", HecServerErrorResponseException.Type.RECOVERABLE_CONFIG_ERROR, ex.getErrorType());
        return true;
    }
}
