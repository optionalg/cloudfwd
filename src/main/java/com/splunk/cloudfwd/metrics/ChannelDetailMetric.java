package com.splunk.cloudfwd.metrics;

import com.splunk.cloudfwd.impl.ConnectionImpl;
import com.splunk.cloudfwd.impl.util.HecChannel;

/**
 * Created by eprokop on 11/15/17.
 */
public class ChannelDetailMetric extends Metric {
    private boolean preFlight_completed;
    private boolean closed;
    private boolean closeFinished;
    private boolean quiesced;
    private boolean healthy;
    private boolean full;
    private boolean misconfigured;
    private boolean dead;
    private boolean decommissioned;
    private boolean available;
    private String channel_id;
    
    public ChannelDetailMetric(ConnectionImpl c, HecChannel channel) {
        super(c);
        this.channel_id = channel.getChannelId();
    }

    @Override
    protected String setType() {
        return "channel_detail";
    }

    public String getId() {
        return channel_id;
    }

    public boolean isPreFlight_completed() {
        return preFlight_completed;
    }

    public boolean isClosed() {
        return closed;
    }

    public boolean isCloseFinished() {
        return closeFinished;
    }

    public boolean isQuiesced() {
        return quiesced;
    }

    public boolean isHealthy() {
        return healthy;
    }

    public boolean isFull() {
        return full;
    }

    public boolean isMisconfigured() {
        return misconfigured;
    }

    public boolean isDead() {
        return dead;
    }

    public boolean isDecommissioned() {
        return decommissioned;
    }

    public boolean isAvailable() {
        return available;
    }

    public void setId(String id) {
        this.channel_id = id;
    }

//    public void setConnectionName(String connection_name) {
//        this.connection_name = connection_name;
//    }
//
//    public void setConnectionAge(long connection_age) {
//        this.connection_age_seconds = connection_age;
//    }

    public void setPreFlightCompleted(boolean preFlight_completed) {
        this.preFlight_completed = preFlight_completed;
    }

    public void setClosed(boolean closed) {
        this.closed = closed;
    }

    public void setCloseFinished(boolean closeFinished) {
        this.closeFinished = closeFinished;
    }

    public void setQuiesced(boolean quiesced) {
        this.quiesced = quiesced;
    }

    public void setHealthy(boolean healthy) {
        this.healthy = healthy;
    }

    public void setFull(boolean full) {
        this.full = full;
    }

    public void setMisconfigured(boolean misconfigured) {
        this.misconfigured = misconfigured;
    }

    public void setDead(boolean dead) {
        this.dead = dead;
    }

    public void setDecommissioned(boolean decommissioned) {
        this.decommissioned = decommissioned;
    }

    public void setAvailable(boolean available) {
        this.available = available;
    }
    
}
