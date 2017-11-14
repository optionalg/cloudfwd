package com.splunk.cloudfwd.metrics;

import com.splunk.cloudfwd.Event;
import com.splunk.cloudfwd.Events;
import com.splunk.cloudfwd.RawEvent;
import com.splunk.cloudfwd.impl.ConnectionImpl;

/**
 * Created by eprokop on 11/14/17.
 */
public class MetricsManager {
    private String connectionName;
    private Long connectionBirthTime;
    private String url;
    private String token;
    
    public MetricsManager(ConnectionImpl c, long birthTime) {
        connectionName = c.toString();
        connectionBirthTime = birthTime;
        url = c.getSettings().getMetricsUrl();
        token = c.getSettings().getMetricsToken();
    }
    
    int emit(Metric metric) {
        metric.setUrl(url);
        metric.setToken(token);
        metric.put(MetricKeys.CONNECTION_AGE, connectionBirthTime.toString());
        metric.put(MetricKeys.CONNECTION_NAME, connectionName); // connection name
        // aggregator will map it to JSON add an ID, and send it
        return MetricsAggregator.emit(metric);
    }
}
