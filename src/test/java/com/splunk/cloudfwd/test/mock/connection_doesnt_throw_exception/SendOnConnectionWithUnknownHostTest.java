package com.splunk.cloudfwd.test.mock.connection_doesnt_throw_exception;

import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.ConnectionSettings;
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
  protected ConnectionSettings getTestProps() {
    ConnectionSettings settings = super.getTestProps();
    settings.setConntctionThrowsExceptionOnCreation(false);
    settings.setUrls("https://foobarunknownhostbaz:8088");
    settings.setMockHttpClassname("com.splunk.cloudfwd.impl.sim.errorgen.discoverer.UnknownHostEndpoints");
    return settings;
  }
  
  @Test
  public void sendThrowsAndHealthContainsExpectedException() throws InterruptedException, HecConnectionTimeoutException {
    super.sendEvents(false, false);
    assertAllChannelsFailed(HecMaxRetriesException.class, "Caused by: java.net.UnknownHostException, with message: Simulated UnknownHostException");
  }
  
  @Override
  protected boolean shouldSendThrowException() {return true;}
  
}
