import com.splunk.cloudfwd.Connections;
import com.splunk.cloudfwd.error.HecConnectionStateException;
import org.junit.Assert;
import org.junit.Test;

import java.net.MalformedURLException;
import java.util.Properties;

import static com.splunk.cloudfwd.PropertyKeys.COLLECTOR_URI;
import static com.splunk.cloudfwd.PropertyKeys.EVENT_BATCH_SIZE;
import static com.splunk.cloudfwd.PropertyKeys.TOKEN;

/**
 * Tests for expected behaviors when trying to instantiate a connection
 * with malformed URLs.
 *
 * Created by eprokop on 10/2/17.
 */
public class BadUrlTest extends AbstractConnectionTest {
    @Override
    protected int getNumEventsToSend() {
        return 10;
    }

    @Test
    public void noProtocol() {
        Properties overrides = new Properties();
        overrides.put(TOKEN, "foo-token");
        overrides.put(COLLECTOR_URI, "bogus_URI");
        try {
            this.connection = Connections.create(callbacks, overrides);
            Assert.fail("It shouldn't be possible to instantiate Connection");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof HecConnectionStateException);
            Assert.assertTrue(((HecConnectionStateException)e).getType()
                == HecConnectionStateException.Type.CONFIGURATION_EXCEPTION);
        }
    }

    @Test
    public void unresolvableHost() {
        Properties overrides = new Properties();
        overrides.put(TOKEN, "foo-token");
        overrides.put(COLLECTOR_URI, "http://fooDoesntExistbar.com");
        try {
            this.connection = Connections.create(callbacks, overrides);
            Assert.fail("It shouldn't be possible to instantiate Connection");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof HecConnectionStateException);
            Assert.assertTrue(((HecConnectionStateException)e).getType()
                == HecConnectionStateException.Type.CONFIGURATION_EXCEPTION);
        }
    }

    @Test
    public void resolvableHost() {
        Properties overrides = new Properties();
        overrides.put(TOKEN, "foo-token");
        overrides.put(COLLECTOR_URI, "http://google.com");
        try {
            this.connection = Connections.create(callbacks, overrides);
            Assert.fail("It shouldn't be possible to instantiate Connection");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof HecConnectionStateException);
            Assert.assertTrue(((HecConnectionStateException)e).getType()
                    == HecConnectionStateException.Type.CONFIGURATION_EXCEPTION);
        }
    }

    // change props valid to invalid
    // change props invalid to valid
    // some urls valid and others invalid
    //
}
