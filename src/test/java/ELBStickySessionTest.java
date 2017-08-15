import com.splunk.cloudfwd.http.EventBatch;
import com.splunk.cloudfwd.http.HttpEventCollectorEvent;
import org.junit.Test;

import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

/**
 * Created by eprokop on 8/14/17.
 */
public class ELBStickySessionTest extends AbstractConnectionTest {
    @Override
    protected Properties getProps() {
        return new Properties();
    }

    @Override
    protected int getNumBatchesToSend() {
        return 1;
    }

    @Override
    protected EventBatch nextEventBatch() {
        EventBatch events = new EventBatch(EventBatch.Endpoint.raw,
                EventBatch.Eventtype.json);
        events.add(new HttpEventCollectorEvent("info", "foo", "HEC_LOGGER",
                Thread.currentThread().getName(), new HashMap(), null, null));
        return events;
    }

    @Test
    public void test() throws InterruptedException, TimeoutException {
        super.sendEvents();
    }
}
