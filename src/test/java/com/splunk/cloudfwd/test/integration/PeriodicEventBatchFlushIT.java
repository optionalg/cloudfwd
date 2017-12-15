package com.splunk.cloudfwd.test.integration;

import com.splunk.cloudfwd.ConnectionSettings;
import com.splunk.cloudfwd.Event;
import com.splunk.cloudfwd.RawEvent;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
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
        sleep(connection.getSettings().getEventBatchFlushTimeout() * 3);
        
        // verify
        Set<String> searchResults = getEventsFromSplunk();
        verifyResults(events, searchResults);
    }

    @Override
    protected void configureProps(ConnectionSettings settings) {
        super.configureProps(settings);
        settings.setEventBatchFlushTimeout(2000);
        settings.setEventBatchSize(100000000); // big enough that we don't fill the batch
        settings.setToken(createTestToken(SINGLE_LINE_SOURCETYPE));
    }
}
