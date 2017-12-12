package com.splunk.cloudfwd.test.integration;

import com.splunk.cloudfwd.Event;
import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.RawEvent;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * Test to make sure that we periodically post events to Splunk, even if buffer size is not reached, 
 * when we internally batch in cloudfwd.
 * 
 * Created by eprokop on 12/11/17.
 */
public class PeriodicEventBatchFlushIT extends AbstractReconciliationTest {
    @Test
    public void testPeriodicFlush() {
        Event e;
        List<Event> events = new ArrayList<>();
        
        // partially fill buffer
        for (int i = 0; i < 100; i++) {
            e = RawEvent.fromText("foo" + Integer.toString(i) + "\n", i);
            events.add(e);
            connection.send(e);
        }
        
        // wait for flush
        sleep(connection.getSettings().getBatchFlushTimeout() * 3);
        
        // verify
        Set<String> searchResults = getEventsFromSplunk();
        verifyResults(events, searchResults);
    }

    @Override
    protected Properties getProps() {
        Properties p = super.getProps();
        p.setProperty(PropertyKeys.EVENT_BATCH_FLUSH_TIMEOUT_MS, "2000");
        p.setProperty(PropertyKeys.EVENT_BATCH_SIZE, "100000000"); // big enough that we don't fill the batch
        p.setProperty(PropertyKeys.TOKEN, createTestToken(SINGLE_LINE_SOURCETYPE));
        return p;
    }
}
