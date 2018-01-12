package com.splunk.cloudfwd;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by eprokop on 12/21/17.
 */
public class LifecycleMetrics {
    public static final String CREATION_TIMESTAMP = "creation_timestamp"; // time the event batch was constructed 
    public static final String START_LB_SPIN_TIMESTAMP = "start_lb_spin_timestamp"; // time the event batch entered the load balancer
    public static final String POST_SENT_TIMESTAMP = "post_sent_timestamp"; // time the event batch post request was sent to Splunk
    public static final String POST_RESPONSE_TIMESTAMP= "post_response_timestamp"; // time the event batch post request received a response
    public static final String ACKED_TIMESTAMP = "acked_timestamp"; // time the event batch was acknowledged
    public static final String FAILED_TIMESTAMP = "failed_timestamp"; // time the event batch was 
    
    private Map<String, List<Long>> timestampsMS = new HashMap<>();
    private long acknowledgedLatency;
    private long postResponseLatency;
    private long timeInLoadBalancer;
    
    
    private EventBatch eb;
            
    public LifecycleMetrics(EventBatch eb) {
        this.eb = eb;
    }
    
    public List<Long> getCreationTimestamp() {
        return timestampsMS.get(CREATION_TIMESTAMP);
    }

    public void setCreationTimestamp(Long timestamp) {
        if (timestampsMS.containsKey(CREATION_TIMESTAMP)) {
            throw new RuntimeException("Illegal attempt to set creation timestamp twice.");
        }
        setTimestamp(CREATION_TIMESTAMP, timestamp);
    }

    public List<Long> getStartLBSpinTimestamp() {
        return timestampsMS.get(START_LB_SPIN_TIMESTAMP);
    }

    public void setStartLBSpinTimestamp(Long timestamp) {
        setTimestamp(START_LB_SPIN_TIMESTAMP, timestamp);
    }

    public List<Long> getPostSentTimestamp() {
        return timestampsMS.get(POST_SENT_TIMESTAMP);
    }

    public void setPostSentTimestamp(Long timestamp) {
        List<Long> lbStartTimes = timestampsMS.get(START_LB_SPIN_TIMESTAMP);
        timeInLoadBalancer = timestamp - lbStartTimes.get(lbStartTimes.size() - 1);
        setTimestamp(POST_SENT_TIMESTAMP, timestamp);
    }

    public List<Long> getPostResponseTimeStamp() {
        return timestampsMS.get(POST_RESPONSE_TIMESTAMP);
    }

    public void setPostResponseTimeStamp(Long timestamp) {
        List<Long> sentTimes = timestampsMS.get(POST_SENT_TIMESTAMP);
        postResponseLatency = timestamp - sentTimes.get(sentTimes.size() - 1);
        setTimestamp(POST_RESPONSE_TIMESTAMP, timestamp);
    }

    public List<Long> getAckedTimestamp() {
        return timestampsMS.get(ACKED_TIMESTAMP);
    }

    public void setAckedTimestamp(Long timestamp) {
        List<Long> sentTimes = timestampsMS.get(POST_SENT_TIMESTAMP);
        acknowledgedLatency = timestamp - sentTimes.get(sentTimes.size() - 1);
        setTimestamp(ACKED_TIMESTAMP, timestamp);
    }

    public List<Long> getFailedTimestamp() {
        return timestampsMS.get(FAILED_TIMESTAMP);
    }

    public void setFailedTimestamp(Long timestamp) {
        setTimestamp(FAILED_TIMESTAMP, timestamp);
    }

    public Long getAcknowledgedLatency() {
        return acknowledgedLatency;
    }

    public Long getPostResponseLatency() {
        return postResponseLatency;
    }
    
    public Long getTimeInLoadBalancer() {
        return timeInLoadBalancer;
    }

    private void setTimestamp(String key, Long timestamp) {
        timestampsMS.putIfAbsent(key, new ArrayList<>());
        timestampsMS.get(key).add(timestamp);
    }
}
