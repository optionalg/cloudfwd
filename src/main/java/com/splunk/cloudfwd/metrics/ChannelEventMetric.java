package com.splunk.cloudfwd.metrics;

import com.splunk.cloudfwd.LifecycleEvent;
import com.splunk.cloudfwd.impl.ConnectionImpl;
import com.splunk.cloudfwd.impl.util.HecChannel;

/**
 * Created by eprokop on 11/15/17.
 */
public class ChannelEventMetric extends Metric {
    private String action;
    private String channel_id;

    public ChannelEventMetric(ConnectionImpl c, HecChannel channel, LifecycleEvent e) {
        super(c);
        this.action = e.getType().toString();
        this.channel_id = channel.getChannelId();
    }

    @Override
    protected String setType() {
        return "channel_event";
    }

    public String getAction() {
        return action;
    }

    public String getId() {
        return channel_id;
    }
}
