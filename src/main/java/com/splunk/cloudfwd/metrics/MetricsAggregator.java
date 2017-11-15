package com.splunk.cloudfwd.metrics;

import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.ConnectionCallbacks;
import com.splunk.cloudfwd.Connections;
import com.splunk.cloudfwd.EventBatch;
import com.splunk.cloudfwd.Events;
import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.RawEvent;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by eprokop on 11/14/17.
 */
public class MetricsAggregator {
    static private Connection connection;
    static private AtomicInteger counter = new AtomicInteger(0);
    
    public static int emit(Metric m) {
        if (connection == null) {
            initConnection(m);
        }
        int sent = 0;
        try {
            sent = connection.send(RawEvent.fromObject(m, counter.incrementAndGet()));
        } catch (IOException e) {
            // no-op 
        }
        return sent;
    }
    
    private static void initConnection(Metric m) {
        Properties p = new Properties();
        p.setProperty(PropertyKeys.COLLECTOR_URI, m.getUrl());
        p.setProperty(PropertyKeys.TOKEN, m.getToken());
        p.setProperty(PropertyKeys.METRICS_ENABLED, "false"); // we don't want metrics for the metrics
        p.setProperty(PropertyKeys.MAX_TOTAL_CHANNELS, "1");
        connection = Connections.create(new MetricsCallbacks(), p);
    }
    
    private static class MetricsCallbacks implements ConnectionCallbacks {
        @Override
        public void acknowledged(EventBatch events) {
            // no-op
        }

        @Override
        public void failed(EventBatch events, Exception ex) {
            // no-op
        }

        @Override
        public void checkpoint(EventBatch events) {
            // no-op
        }

        @Override
        public void systemError(Exception e) {
            // no-op
        }

        @Override
        public void systemWarning(Exception e) {
            // no-op
        }
    }
}
