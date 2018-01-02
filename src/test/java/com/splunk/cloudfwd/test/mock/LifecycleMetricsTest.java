package com.splunk.cloudfwd.test.mock;

import com.splunk.cloudfwd.EventBatch;
import com.splunk.cloudfwd.Events;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by eprokop on 12/21/17.
 */
public class LifecycleMetricsTest {
    EventBatch eb;
    
    @Before
    public void createBatch() {
       this.eb = Events.createBatch();
    }
    
    @Test
    public void testCreationTimestamp() {
        Exception e = null;
        try {
            eb.getLifecycleMetrics().setCreationTimestamp(System.currentTimeMillis());
        } catch (RuntimeException ex) {
            e = ex;
        }
        Assert.assertNotNull("Expected exception from illegally setting timestamp", e);
        Assert.assertNotNull("creation timestamp shouldn't be null", eb.getLifecycleMetrics().getCreationTimestamp());
    }
    
    @Test
    public void testAckedTimestamp() {
        long time = System.currentTimeMillis();
        eb.getLifecycleMetrics().setStartLBSpinTimestamp(time - 1);
        eb.getLifecycleMetrics().setPostSentTimestamp(time);
        eb.getLifecycleMetrics().setAckedTimestamp(time + 1);
        Assert.assertEquals("acked latency should be 1", 1, (long)eb.getLifecycleMetrics().getAcknowledgedLatency());
        Assert.assertEquals("timestamps should match", time + 1, (long)eb.getLifecycleMetrics().getAckedTimestamp().get(0));
    }

    @Test
    public void testFailedTimestamp() {
        long time = System.currentTimeMillis();
        eb.getLifecycleMetrics().setFailedTimestamp(time);
        Assert.assertEquals("timestamps should match", time, (long)eb.getLifecycleMetrics().getFailedTimestamp().get(0));
    }
    
    @Test
    public void testEnteredLoadBalancerTimestamp() {
        long time = System.currentTimeMillis();
        eb.getLifecycleMetrics().setStartLBSpinTimestamp(time);
        eb.getLifecycleMetrics().setStartLBSpinTimestamp(time + 1);
        eb.getLifecycleMetrics().setPostSentTimestamp(time + 2);
        Assert.assertEquals("timestamps should match", time, (long)eb.getLifecycleMetrics().getStartLBSpinTimestamp().get(0));
        Assert.assertEquals("timestamps should match", time + 1, (long)eb.getLifecycleMetrics().getStartLBSpinTimestamp().get(1));
        Assert.assertEquals("time in load balancer is incorrect", 1, (long)eb.getLifecycleMetrics().getTimeInLoadBalancer());
    }
    
    @Test
    public void testPostResponseTimestamp() {
        long time = System.currentTimeMillis();
        eb.getLifecycleMetrics().setStartLBSpinTimestamp(time - 1);
        eb.getLifecycleMetrics().setPostSentTimestamp(time);
        eb.getLifecycleMetrics().setPostResponseTimeStamp(time + 1);
        Assert.assertEquals("timestamps should match", time, (long)eb.getLifecycleMetrics().getPostSentTimestamp().get(0));
        Assert.assertEquals("timestamps should match", time + 1, (long)eb.getLifecycleMetrics().getPostResponseTimeStamp().get(0));
        Assert.assertEquals("latency should be 1", 1, (long)eb.getLifecycleMetrics().getPostResponseLatency());
    }
}
