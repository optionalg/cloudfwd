import com.splunk.cloudfwd.Connections;
import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.error.HecConnectionStateException;
import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import org.junit.Test;

import java.util.Properties;
import java.util.Set;

import static com.splunk.cloudfwd.error.HecConnectionStateException.Type.CONFIGURATION_EXCEPTION;

/**
 * Created by eprokop on 10/5/17.
 */
public class CreateConnectionSomeUnknownHostsIT extends AbstractReconciliationTest {
    private String unknownHost = "https://foobarunknownhostbaz:8088";

    // Scenario: urls with unknown hosts are provided, but there is at least one "good" url
    // Expected behavior: Connection should instantiate and all events should make it into Splunk. "Unknown host" error should surface through systemError() callback
    @Test
    public void createConnectionWithMixedUnknownHostAndValidURLs() throws InterruptedException, HecConnectionTimeoutException {
        super.sendEvents();
        Set<String> searchResults = getEventsFromSplunk();
        verifyResults(getSentEvents(), searchResults);
    }

    @Override
    protected int getNumEventsToSend() {
        return 10;
    }

    @Override
    protected Properties getProps() {
        Properties p = super.getProps();
        p.setProperty(PropertyKeys.TOKEN, createTestToken("__singleline"));
        p.setProperty(PropertyKeys.COLLECTOR_URI, unknownHost + ",https://localhost:8088");
        p.setProperty(PropertyKeys.MAX_TOTAL_CHANNELS, "2");
        return p;
    }

    @Override
    protected BasicCallbacks getCallbacks() {
        return new BasicCallbacks(getNumEventsToSend()) {
            @Override
            public boolean shouldFail(){
                return true;
            }

            protected boolean isExpectedFailureType(Exception e){
                if (e instanceof HecConnectionStateException) {
                    return ((HecConnectionStateException)e).getType() == CONFIGURATION_EXCEPTION
                        && e.getMessage().equals("Unknown host. " + unknownHost);
                }
                return false;
            }
        };
    }
}

