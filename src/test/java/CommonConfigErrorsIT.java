import com.splunk.cloudfwd.*;
import com.splunk.cloudfwd.error.HecNoValidChannelsException;
import com.splunk.cloudfwd.error.HecServerErrorResponseException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by eprokop on 10/4/17.
 */
public class CommonConfigErrorsIT extends AbstractReconciliationTest {
    @Override
    protected int getNumEventsToSend() {
        return 10;
    }

    @Before
    @Override
    public void setUp() {
        this.callbacks = getCallbacks();
        this.testMethodGUID = java.util.UUID.randomUUID().toString();
        this.events = new ArrayList<>();
    }

    @Test
    public void sendRawByteBufferToEventsEndpoint() {
        Properties props = new Properties();
        props.putAll(getTestProps());
        props.putAll(getProps());
        props.put(PropertyKeys.TOKEN, createTestToken(null));
        this.connection = Connections.create(getCallbacks(), props);
        connection.getSettings().setHecEndpointType(Connection.HecEndpoint.STRUCTURED_EVENTS_ENDPOINT);
        super.eventType = Event.Type.UNKNOWN;
        String eventText = "foo";
        Event e = new UnvalidatedBytesEvent(eventText.toString().getBytes(), 0);
        EventBatch b = Events.createBatch();
        b.add(e);
        connection.sendBatch(b);
    }

//    @Test
//    public void toggleTokenValidity() {
//        Properties p = new Properties();
//        p.put(PropertyKeys.TOKEN, "invalid_token");
//        Connection c = Connections.create(getCallbacks(), p);
//        // preflight checks should fail, and we should not start health polling.
//        // all channels should be effectively dead.
//        List<HecHealth> channelHealths = c.getHealth();
//        Assert.assertTrue(channelHealths.size() > 0);
//        for (HecHealth h : channelHealths) {
//            Assert.assertTrue(!h.isHealthy());
//            Assert.assertTrue(h.isMisconfigured());
//            Assert.assertTrue(h.getStatus().getType() == LifecycleEvent.Type.INVALID_TOKEN);
//            Assert.assertTrue(h.getStatusException() instanceof HecServerErrorResponseException);
//            HecServerErrorResponseException e = (HecServerErrorResponseException)h.getStatusException();
//            // check code?
//        }
//
//        // check to make sure we throw proper exception on send
//        EventBatch batch  = Events.createBatch();
//        batch.add(RawEvent.fromText("foo", 0));
//        Exception e = null;
//        try {
//            c.sendBatch(batch);
//        } catch(HecNoValidChannelsException ex) {
//            e = ex;
//        }
//        Assert.assertTrue("Expected HecNoValidChannelsException was never thrown.", e != null);
//
//        // check for consistency between getHealth() API and
//        for (HecHealth h : ((HecNoValidChannelsException) e).getHecHealthList()) {
//            Assert.assertTrue(!h.isHealthy());
//            Assert.assertTrue(h.isMisconfigured());
//            Assert.assertTrue(h.getStatus().getType() == LifecycleEvent.Type.INVALID_TOKEN);
//            Assert.assertTrue(h.getStatusException() instanceof HecServerErrorResponseException);
//            // check code?
//        }
//
//    }

}
