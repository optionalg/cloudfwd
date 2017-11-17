package com.splunk.cloudfwd.metrics;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.splunk.cloudfwd.impl.ConnectionImpl;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by eprokop on 11/16/17.
 */
public class ConnectionsSummaryMetric extends Metric {
    private long jvm_elapsed_seconds;
    private List<ConnectionDetailMetric> connectionDetailMetricList = new ArrayList<>();
    
    public ConnectionsSummaryMetric(long startTime) {
        this.jvm_elapsed_seconds = (System.currentTimeMillis() - startTime) / 1000;
    }
    
    public void add(ConnectionDetailMetric m) {
        connectionDetailMetricList.add(m);
    }
    
    @Override
    protected String setType() {
        return "connections_summary";
    }

    public long getJvm_elapsed_seconds() {
        return jvm_elapsed_seconds;
    }

    public void setJvm_elapsed_seconds(long jvm_elapsed_seconds) {
        this.jvm_elapsed_seconds = jvm_elapsed_seconds;
    }
    
    public List<ConnectionDetailMetric> getConnectionDetailMetricList() {
        return connectionDetailMetricList;
    }
    
    public int getNumConnections() {
        return connectionDetailMetricList.size();
    }
    
    // Override so we don't output these when mapping to JSON object 
    @Override
    @JsonIgnore
    public String getMetricSinkUrl() {
        return "";
    }

    @Override
    @JsonIgnore
    public String getMetricSinkToken() {
        return "";
    }

    @Override
    @JsonIgnore
    public long getConnectionAgeSeconds() {
        return -1;
    }

    @Override
    @JsonIgnore
    public String getConnectionName() {
        return "";
    }
}
