package com.splunk.cloudfwd.metrics;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by eprokop on 11/14/17.
 */

public class Metric {
    private final Map<String, String> map = new HashMap<>();
    private String url;
    private String token;
    
    public Metric(String key, String value) {
        map.put(key, value);
    }
    
    public void put(String key, String value) {
        map.put(key, value);
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }
}
