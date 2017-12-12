package com.splunk.cloudfwd.test.mock;
        
        import com.splunk.cloudfwd.PropertyKeys;
        import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
        import com.splunk.cloudfwd.error.HecIllegalStateException;
        import com.splunk.cloudfwd.error.HecMaxRetriesException;
        import com.splunk.cloudfwd.error.HecNoValidChannelsException;
        import org.junit.Test;
        
        import java.util.Properties;

/**
 * Created by eprokop on 10/9/17.
 */
public class SendOnConnectionWithNoRouteToHost extends AbstractExceptionOnSendTest {
  // Scenario: Hostname can be resolved but not reached
  // Expected behavior: Connection succeeds to instantiate and send throws a proper exception
  @Test
  public void sendThrowsAndHealthContainsExpectedException() throws InterruptedException, HecConnectionTimeoutException {
    super.sendEvents(false, false);
    assertAllChannelsFailed(HecMaxRetriesException.class,"No route to host");
    connection.close();
  }
  
  @Override
  protected Properties getProps() {
    Properties p = super.getProps();
    p.setProperty(PropertyKeys.MOCK_HTTP_KEY, "true");
    p.setProperty(PropertyKeys.MOCK_HTTP_CLASSNAME, "com.splunk.cloudfwd.impl.sim.errorgen.splunkcheckfailure.NoRouteToHostEndpoints");
    p.setProperty(PropertyKeys.EVENT_BATCH_SIZE, "0");
    p.setProperty(PropertyKeys.COLLECTOR_URI, "https://localhost:8088/");
    p.setProperty(PropertyKeys.ACK_TIMEOUT_MS, "3000");
    return p;
  }
  
  @Override
  protected int getNumEventsToSend() {
    return 1;
  }
  
  @Override
  protected boolean shouldSendThrowException() {return true;}
  
  @Override
  protected boolean isExpectedSendException(Exception ex) {
    LOG.trace("isExpectedSendException: " + (ex instanceof HecIllegalStateException) + ", ex: " + ex);
    return ex instanceof HecNoValidChannelsException;
  }
  
}
