package com.splunk.cloudfwd.metrics;

import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.Connections;

import java.util.Properties;

/**
 * Created by eprokop on 11/14/17.
 */
public class MetricsAggregator {
    static {
        Properties p = new Properties(); 
        Connection connection = Connections.create(p);
    }
    
    public static int emit(Metric m) {
        
    }
}
