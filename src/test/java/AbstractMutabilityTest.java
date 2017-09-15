import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.Event;
import com.splunk.cloudfwd.HecConnectionTimeoutException;
import org.junit.Assert;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Created by eprokop on 9/11/17.
 */
public abstract class AbstractMutabilityTest extends AbstractConnectionTest {
    private int start = 0;
    private int stop = -1;

    abstract protected int getNumEventsToSend();

    @Override
    protected void configureConnection(Connection connection) {
        connection.getSettings().setEventBatchSize(1024*32); //32k batching batching, roughly
    }

    @Override
    abstract protected Properties getProps();

    protected void sendSomeEvents(int numEvents) throws InterruptedException, HecConnectionTimeoutException {
        System.out.println(
                "SENDING EVENTS WITH CLASS GUID: " + TEST_CLASS_INSTANCE_GUID
                        + "And test method GUID " + testMethodGUID);

        stop += numEvents;
        for (int i = start; i <= stop; i++) {
            Event event = nextEvent(i + 1);
            connection.send(event);
        }
        start = stop + 1;
    }

    protected void close() throws InterruptedException, HecConnectionTimeoutException {
        connection.close(); //will flush
        this.callbacks.await(10, TimeUnit.MINUTES);
        if (callbacks.isFailed()) {
            Assert.fail("There was a failure callback with exception class  "
                    + callbacks.getException() + " and message " + callbacks.getFailMsg());
        }
    }

    protected void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }
}
