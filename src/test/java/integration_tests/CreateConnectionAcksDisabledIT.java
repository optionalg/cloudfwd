package integration_tests;

import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.Connections;
import com.splunk.cloudfwd.LifecycleEvent;
import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.error.HecConnectionStateException;
import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import com.splunk.cloudfwd.error.HecServerErrorResponseException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Properties;
import java.util.Set;

import static com.splunk.cloudfwd.error.HecConnectionStateException.Type.CONFIGURATION_EXCEPTION;

/**
 * Created by eprokop on 10/4/17.
 */
public class CreateConnectionAcksDisabledIT extends AbstractReconciliationTest {
    @Override
    protected int getNumEventsToSend() {
        return 0;
    }

    // TODO: this doesn't currently work because health polling doesn't know about acks disabled and
    // TODO: still turns on after preflight failure, so channels get marked as healthy
//    @Test
//    public void createConnectionWithAcksDisabled() {
//        createTestIndex();
//        Properties p = getPropertiesObjectWithDefaults();
//        p.put(PropertyKeys.TOKEN, createTestToken(null, false));
//
//        HecServerErrorResponseException ex = null;
//        try {
//            this.connection = Connections.create(getCallbacks(), p);
//        } catch(HecServerErrorResponseException e) {
//            ex = e;
//        }
//        Assert.assertNotNull("Exception should be thrown when creating connection with token with acks disabled on all channels.", ex);
//        Assert.assertEquals(LifecycleEvent.Type.ACK_DISABLED, ex.getLifecycleType());
//        Assert.assertEquals(HecServerErrorResponseException.Type.RECOVERABLE_CONFIG_ERROR, ex.getErrorType());
//    }

    // TODO: url test for unreachable, and also malformed.
    // TODO: test NO_CHANNELS type

    // TODO: we need tests that test if SOME channels have ack disabled/token invalid and others don't.
}
