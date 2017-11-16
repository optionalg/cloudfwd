package com.splunk.cloudfwd.metrics;

import com.splunk.cloudfwd.impl.ConnectionImpl;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by eprokop on 11/16/17.
 */
public class ConnectionsSummaryMetric { // don't extend Metric since it makes no sense for this to take a ConnectionImpl in constructor
    private long jvm_elapsed_seconds;
    private String type = setType();
    private List<ConnectionDetailMetric> connectionDetailMetricList = new ArrayList<>();
    
    public ConnectionsSummaryMetric(long startTime) {
        this.jvm_elapsed_seconds = (System.currentTimeMillis() - startTime) / 1000;
    }
    
    public void add(ConnectionDetailMetric m) {
        connectionDetailMetricList.add(m);
    }
    
    private String setType() {
        return "connections_summary";
    }
    
    private String getType() {
        return type;
    }
}
