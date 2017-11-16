package com.splunk.cloudfwd.metrics;

import com.splunk.cloudfwd.impl.ConnectionImpl;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by eprokop on 11/15/17.
 */
public class ChannelSummaryMetric extends Metric {
    private List<ChannelDetailMetric> channelDetailList = new ArrayList<>();
    
    public ChannelSummaryMetric(ConnectionImpl c) {
        super(c);
    }

    @Override
    protected String setType() {
        return "channels_summary";
    }
    
    public void add(ChannelDetailMetric m) {
        channelDetailList.add(m);
    }
    
    public List<ChannelDetailMetric> getChannelDetailList() {
        return channelDetailList;
    }
}
