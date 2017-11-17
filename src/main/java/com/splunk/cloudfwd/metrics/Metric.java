package com.splunk.cloudfwd.metrics;

import com.splunk.cloudfwd.impl.ConnectionImpl;

/**
 * Created by eprokop on 11/14/17.
 */

public abstract class Metric {
    private String type = setType();
    private String metricSinkUrl;
    private String metricSinkToken;
    private String connection_name;
    private long connection_age_seconds;
    private String run_id;
    
    public Metric() {
        
    }
    
    public Metric(ConnectionImpl c) {
        this.connection_name = c.toString();
        this.connection_age_seconds = c.getAgeSeconds();
    }
    
    protected abstract String setType();
    
//    public void put(String key, String value) {
//        map.put(key, value);
//    }

    public String getMetricSinkUrl() {
        return metricSinkUrl;
    }

    public void setUrl(String metricSinkUrl) {
        this.metricSinkUrl = metricSinkUrl;
    }

    public String getMetricSinkToken() {
        return metricSinkToken;
    }

    public void setToken(String metricSinkToken) {
        this.metricSinkToken = metricSinkToken;
    }

    public String getType() {
        return type;
    }

    public String getConnectionName() {
        return connection_name;
    }

    public long getConnectionAgeSeconds() {
        return connection_age_seconds;
    }

    public String getRunId() {
        return run_id;
    }

    public void setRunId(String run_id) {
        this.run_id = run_id;
    }
}
