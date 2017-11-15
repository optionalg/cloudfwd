package com.splunk.cloudfwd.metrics;

import com.splunk.cloudfwd.Event;
import com.splunk.cloudfwd.Events;
import com.splunk.cloudfwd.RawEvent;
import com.splunk.cloudfwd.impl.ConnectionImpl;
import com.splunk.cloudfwd.impl.util.HecChannel;
import org.slf4j.Logger;

/**
 * Created by eprokop on 11/14/17.
 */
public class MetricsManager {
    private final Logger LOG;
    
    private String connectionName;
    private Long connectionBirthTime;
    private String url; // destination HEC url for metrics
    private String token;
    
    public MetricsManager(ConnectionImpl c, long birthTime) {
        this.LOG = c.getLogger(MetricsManager.class.getName());
        this.connectionName = c.toString();
        this.connectionBirthTime = birthTime;
        this.url = c.getSettings().getMetricsUrl();
        this.token = c.getSettings().getMetricsToken();
    }
    
    public void emit(Metric metric) {
        metric.setUrl(this.url);
        metric.setToken(this.token);
        metric.put(MetricKeys.CONNECTION_AGE, connectionBirthTime.toString());
        metric.put(MetricKeys.CONNECTION_NAME, connectionName); // connection name
        // aggregator will map it to JSON add an ID, and send it
        if (MetricsAggregator.emit(metric) == 0) {
            LOG.warn("Problem sending metric: " + metric);
        }
    }
}
