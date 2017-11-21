package com.splunk.cloudfwd.test.mock;

import com.splunk.cloudfwd.HecHealth;
import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.error.HecConnectionStateException;
import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import com.splunk.cloudfwd.error.HecMaxRetriesException;
import com.splunk.cloudfwd.error.HecNoValidChannelsException;
import org.junit.Assert;
import org.junit.Test;

import javax.net.ssl.SSLPeerUnverifiedException;
import java.util.List;
import java.util.Properties;

import static com.splunk.cloudfwd.PropertyKeys.MOCK_HTTP_CLASSNAME;
import static com.splunk.cloudfwd.PropertyKeys.MOCK_HTTP_KEY;
import static com.splunk.cloudfwd.error.HecConnectionStateException.Type.CONFIGURATION_EXCEPTION;

/**
 * Scenario: Unknown host provided (no "good" URLs)
 * Expected behavior: Connection succeedes and send throws expected exception
 *
 * Created by eprokop on 10/5/17.
 */
public class CreateConnectionUnknownHostTest extends ExceptionConnInstantiationTest {
  @Override
  protected Properties getProps() {
      Properties props = new Properties();
      props.put(PropertyKeys.COLLECTOR_URI, "https://foobarunknownhostbaz:8088");
      props.put(MOCK_HTTP_KEY, "true");
      props.put(MOCK_HTTP_CLASSNAME,
            "com.splunk.cloudfwd.impl.sim.errorgen.discoverer.InvalidHostName");
      return props;
  }
  
  @Test
  public void sendThrowsAndHealthContainsExpectedException() throws InterruptedException, HecConnectionTimeoutException {
    super.sendEvents(false, false);
    List<HecHealth> healths = connection.getHealth();
    Assert.assertTrue(!healths.isEmpty());
    // we expect all channels to fail catching SSLPeerUnverifiedException in preflight 
    Assert.assertTrue(healths.stream()
            .map(h -> h.getStatus().getException())
            .filter(e -> e instanceof HecMaxRetriesException)
            .filter(e -> e.getMessage().equals("URI does not specify a valid host name: foobarunknownhostbaz:8088/services/collector/ack"))
            .count() == healths.size());
  }
  
  @Override
  protected boolean shouldSendThrowException() {return true;}
  
  @Override
  protected boolean isExpectedSendException(Exception e) {
    return e instanceof HecNoValidChannelsException; 
  }
  
}
