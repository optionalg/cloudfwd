import com.splunk.cloudfwd.LifecycleEvent;
import com.splunk.cloudfwd.PropertyKeys;
import org.junit.Test;

import java.util.Properties;

import static com.splunk.cloudfwd.PropertyKeys.BLOCKING_TIMEOUT_MS;
import static com.splunk.cloudfwd.PropertyKeys.MOCK_HTTP_CLASSNAME;

/**
 * Created by mhora on 10/4/17.
 */
public class InDetentionAllTest extends AbstractInDetentionTest{

    @Override
    protected BasicCallbacks getCallbacks() {
        return new BasicCallbacks(getNumEventsToSend()) {

            @Override
            public boolean shouldWarn(){
                return true; //each failed preflight test will return INDEXER_IN_DETENTION via a systemWarning callback
            }

        };
    }

    @Override
    protected Properties getProps() {
        Properties props = new Properties();
                props.put(MOCK_HTTP_CLASSNAME,
                        "com.splunk.cloudfwd.impl.sim.errorgen.indexer.InDetentionEndpoints");
        props.put(BLOCKING_TIMEOUT_MS, "30000");
        props.put(PropertyKeys.UNRESPONSIVE_MS, "-1"); //no dead channel detection
        props.put(PropertyKeys.MAX_TOTAL_CHANNELS, "2");

        return props;
    }

    @Override
    protected boolean shouldSendThrowException() { //fixme todo - it ain't even gonna get to send. It will fail fast instantiating connection
        return true;
    }

    @Test
    public void sendToIndexersInDetention() throws InterruptedException {
        createConnection(LifecycleEvent.Type.INDEXER_IN_DETENTION);
    }
}
