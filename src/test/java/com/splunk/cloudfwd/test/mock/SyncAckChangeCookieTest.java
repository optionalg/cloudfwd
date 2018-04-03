package com.splunk.cloudfwd.test.mock;

import com.splunk.cloudfwd.Event;
import com.splunk.cloudfwd.HecHealth;
import com.splunk.cloudfwd.impl.sim.errorgen.cookies.SyncAckedEndpoint;
import com.splunk.cloudfwd.impl.util.HecHealthImpl;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


public class SyncAckChangeCookieTest extends SyncAckTest {

    private static final Logger LOG = LoggerFactory.getLogger(SyncAckChangeCookieTest.class.getName());
  
  /**
   *   
   * @param 
   * @return
   */
    @Test
    @Override
    public void testSyncAck() throws InterruptedException {
      List<String> listofChannelIds = getChannelId(this.connection);
      sendEvents(false, false); //do not close connection till we validate state at the end of the test
      verifySyncAckConditions();
      List<String> listofChannelsAfterCookieChanges = getChannelId(this.connection);
      for (String i : listofChannelsAfterCookieChanges) {
        if (listofChannelIds.contains(i)) {
          Assert.fail("Channel Id never changed after toggling cookies.");
        }
      }
      connection.close();
    }
    
    protected Event nextEvent(int i) {
        if (0 == i % (getNumEventsToSend() / 10)) {
            LOG.debug("SyncAckTest: Toggling cookies twice while sending message: {}", i);
            SyncAckedEndpoint.toggleCookie();
        }
        List<HecHealth> healths = connection.getHealth(); 
        LOG.debug("SyncAckTest: channels_total=" + healths.stream().count() + ", channels_healthy=" + healths.stream().filter(h->((HecHealthImpl)h).isHealthy()).count());
        return super.nextEvent(i);
    }
}