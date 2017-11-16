package com.splunk.cloudfwd.metrics;

import com.splunk.cloudfwd.ConnectionCallbacks;
import com.splunk.cloudfwd.EventBatch;
import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.RawEvent;
import com.splunk.cloudfwd.impl.ConnectionImpl;
import com.splunk.cloudfwd.impl.util.ThreadScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by eprokop on 11/14/17.
 */
public class MetricsAggregator {
    private static final Logger LOG = LoggerFactory.getLogger(MetricsAggregator.class.getName());
    /*
    SHOULD we just be sending events?
    
    // ideally want to show a list of channels, and how long they've been alive (birthTime), their health, and how many acks/failures/events
    ex. 
    {
        metricsConnection: <name>
        channel: <name>
        event: <failure|ack|event_post|pre-flight-ok|>
    }
       
    GLOBAL SUMMARY (per metricsConnection - basically logLBhealth)
    Splunk search would basically pipe to "head 1" to always have the most up to date information
        {
            type: connection_summary
            metricsConnection: <name>
            numChannels: <num-channels>
            age_minutes:
            ...
        }
        
    PER_CHANNEL METRIC
        {
            channel: <name>
            metricsConnection: <name>
            num_sent: 
            num_acknowledged: 
            num_failed:
            age:
            time:
        }
    */
     // TODO: the single channel is often full.. (max unacked count == 0) 
    // TODO: periodically emit a ConnectionSummaryMetric that lists all connections and number of channels (maybe number of threads?)
    static private MetricsConnection metricsConnection;
    static private AtomicInteger eventIdCounter = new AtomicInteger(0);
    static private Set<ConnectionImpl> connectionSet = new ConcurrentSkipListSet<>(); // references to Connection instances for which this class sends metrics
    static final private String run_id = java.util.UUID.randomUUID().toString();
    static final private long startTime = System.currentTimeMillis();
    static private ScheduledExecutorService executor;
    
    synchronized public static int emit(Metric m) {
        if (metricsConnection == null) {
            initConnection(m);
            start();
        }
        int sent = 0;
        try {
            m.setRunId(run_id);
            sent = metricsConnection.send(RawEvent.fromObject(m, eventIdCounter.incrementAndGet()));
        } catch (IOException e) {
            // no-op 
        }
        return sent;
    }
    
    private static void start() {
        executor = ThreadScheduler.getSharedSchedulerInstance("metrics_global_reporter");
        Runnable flushAndReportGlobalMetrics = () -> {
            metricsConnection.flush();
            ConnectionsSummaryMetric csm = new ConnectionsSummaryMetric(startTime);
            connectionSet.forEach((c)-> {
                csm.add(new ConnectionDetailMetric(c));
            });
            emit(csm);
        };
        executor.schedule(flushAndReportGlobalMetrics, 30, TimeUnit.SECONDS);
    }
    
    private static void stop() {
        executor.shutdown();
    }
    
    private static void initConnection(Metric m) {
        Properties p = new Properties();
        p.setProperty(PropertyKeys.COLLECTOR_URI, m.getUrl());
        p.setProperty(PropertyKeys.TOKEN, m.getToken());
        p.setProperty(PropertyKeys.ENABLE_CHECKPOINTS, "false");
        p.setProperty(PropertyKeys.METRICS_ENABLED, "false"); // we don't want metrics for the metrics
        p.setProperty(PropertyKeys.MAX_TOTAL_CHANNELS, "1");
//        p.setProperty(PropertyKeys.EVENT_BATCH_SIZE, "512000"); // 500 KB batch sizes
        p.setProperty(PropertyKeys.EVENT_BATCH_SIZE, "50000"); 
        metricsConnection = new MetricsConnection(new MetricsCallbacks(), p);
        LOG.info("MetricsConnection instantiated");
        LOG.info("run_id={}", run_id);
    }
    
    public static void registerConnection(ConnectionImpl c) {
        connectionSet.add(c); // TODO: java.lang.ClassCastException: com.splunk.cloudfwd.impl.ConnectionImpl cannot be cast to java.lang.Comparable
    }

    synchronized public static void deRegisterConnection(ConnectionImpl c) {
        connectionSet.remove(c);
        if (connectionSet.isEmpty()) {
            metricsConnection.closeNow();
            metricsConnection = null;
            stop();
            LOG.trace("MetricsConnection closed and MetricsAggregator scheduler shutdown");
        }
    }
    
    private static class MetricsCallbacks implements ConnectionCallbacks {
        private static final Logger LOG = LoggerFactory.getLogger(MetricsCallbacks.class.getName());
        
        @Override
        public void acknowledged(EventBatch events) {
            // no-op
        }

        @Override
        public void failed(EventBatch events, Exception ex) {
            LOG.error("Failed metrics event batch {} with exception {}", events, ex);
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
