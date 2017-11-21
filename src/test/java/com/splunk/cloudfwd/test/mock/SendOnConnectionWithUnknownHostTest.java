package com.splunk.cloudfwd.test.mock;

import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import com.splunk.cloudfwd.error.HecMaxRetriesException;
import org.junit.Test;

import java.util.Properties;

import static com.splunk.cloudfwd.PropertyKeys.MOCK_HTTP_CLASSNAME;

/**
 * Scenario: Unknown host provided (no "good" URLs)
 * Expected behavior: Connection instantiation succeeds and send throws expected exception
 *
 * Created by eprokop on 10/5/17.
 */
public class SendOnConnectionWithUnknownHostTest extends AbstractExceptionOnSendTest {

  @Override
  protected Properties getProps() {
      Properties props = new Properties();
      props.put(PropertyKeys.COLLECTOR_URI, "https://foobarunknownhostbaz.com:8088/");
      props.put(MOCK_HTTP_CLASSNAME, "com.splunk.cloudfwd.impl.sim.errorgen.discoverer.UnknownHostEndpoints");
      props.put(PropertyKeys.EVENT_BATCH_SIZE, "0");
      return props;
  }
  
  @Test
  public void sendThrowsAndHealthContainsExpectedException() throws InterruptedException, HecConnectionTimeoutException {
    super.sendEvents(false, false);
    assertAllChannelsFailed(HecMaxRetriesException.class, "Simulated UnknownHostException");
    connection.close();
  }
  
  @Override
  protected boolean shouldSendThrowException() {return true;}
  
}
