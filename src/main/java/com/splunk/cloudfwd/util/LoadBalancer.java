/*
 * Copyright 2017 Splunk, Inc..
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.splunk.cloudfwd.util;

import com.splunk.cloudfwd.EventBatch;
import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.HecConnectionTimeoutException;
import com.splunk.cloudfwd.HecConnectionStateException;
import com.splunk.cloudfwd.HecIllegalStateException;
import com.splunk.cloudfwd.PropertyKeys;
import static com.splunk.cloudfwd.PropertyKeys.MAX_TOTAL_CHANNELS;
import com.splunk.cloudfwd.http.HttpSender;

import static com.splunk.cloudfwd.http.lifecycle.LifecycleEvent.Type.EVENT_POST_FAILURE;
import java.io.Closeable;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author ghendrey
 */
public class LoadBalancer implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(
            LoadBalancer.class.
            getName());
    private int channelsPerDestination;
    private final Map<String, HecChannel> channels = new ConcurrentHashMap<>();
    private final Map<String, HecChannel> channelsStaleUrls = new ConcurrentHashMap<>();
    private final CheckpointManager checkpointManager; //consolidate metrics across all channels
    private final IndexDiscoverer discoverer;
    private final IndexDiscoveryScheduler discoveryScheduler = new IndexDiscoveryScheduler();
    private int robin; //incremented (mod channels) to perform round robin
    private final Connection connection;
    private boolean closed;
    private volatile CountDownLatch latch;

    public LoadBalancer(Connection c) {
        this.connection = c;
        this.channelsPerDestination = c.getPropertiesFileHelper().
                getChannelsPerDestination();
        this.discoverer = new IndexDiscoverer(c.getPropertiesFileHelper());
        this.checkpointManager = new CheckpointManager(c);
        //this.discoverer.addObserver(this);
    }

    private void updateChannels(IndexDiscoverer.Change change) {
        LOG.debug(change.toString());
    }

    public synchronized void sendBatch(EventBatch events) throws HecConnectionTimeoutException {
        if (null == this.connection.getCallbacks()) {
            throw new HecConnectionStateException(
                    "Connection FutureCallback has not been set.",
                    HecConnectionStateException.Type.CONNECTION_CALLBACK_NOT_SET);
        }
        if (channels.isEmpty()) {
            createChannels(discoverer.getAddrs());
        }
        sendRoundRobin(events);
    }

    @Override
    public synchronized void close() {
        this.discoveryScheduler.stop();
        for (HecChannel c : this.channels.values()) {
            c.close();
        }
        this.closed = true;
    }

    public synchronized void closeNow() {
        this.discoveryScheduler.stop();
        //synchronized (channels) {
        for (HecChannel c : this.channels.values()) {
            c.forceClose();
        }
        for (HecChannel c : this.channelsStaleUrls.values()) {
            c.forceClose();
        }
        //}
        this.closed = true;
    }

    public CheckpointManager getCheckpointManager() {
        return this.checkpointManager;
    }

    private synchronized void createChannels(List<InetSocketAddress> addrs) {
        for (InetSocketAddress s : addrs) {
            //add multiple channels for each InetSocketAddress
            for (int i = 0; i < channelsPerDestination; i++) {
                addChannel(s, false);
            }
        }
    }

    void addChannelFromRandomlyChosenHost() {
        InetSocketAddress addr = discoverer.randomlyChooseAddr();
        LOG.debug("Adding channel to {0}", addr);
        addChannel(addr, true); //this will force the channel to be added, even if we are ac MAX_TOTAL_CHANNELS
    }

    private synchronized void addChannel(InetSocketAddress s, boolean force) {
        //sometimes we need to force add a channel. Specifically, when we are replacing a reaped channel
        //we must add a new one, before we cancelEventTrackers the old one. If we did not have the force
        //argument, adding the new channel would get ignored if MAX_TOTAL_CHANNELS was set to 1,
        //and then the to-be-reaped channel would also be removed, leaving no channels, and
        //send will be stuck in a spin loop with no channels to send to
        PropertiesFileHelper propsHelper = this.connection.
                getPropertiesFileHelper();
        if (!force && channels.size() >= propsHelper.getMaxTotalChannels()) {
            LOG.debug(
                    "Can't add channel (" + MAX_TOTAL_CHANNELS + " set to " + propsHelper.
                    getMaxTotalChannels() + ")");
            return;
        }
        URL url;
        String host;
        try {
            //URLS for channel must be based on IP address not hostname since we
            //have many-to-one relationship between IP address and hostname via DNS records

            url = new URL(
                    "https://" + s.getAddress().getHostAddress() + ":" + s.
                    getPort());
            LOG.debug("Trying to add URL: " + url);
            //We should provide a hostname for http client, so it can properly set Host header
            //this host is required for many proxy server and virtual servers implementations
            //https://tools.ietf.org/html/rfc7230#section-5.4
            host = s.getHostName() + ":" + s.getPort();

            HttpSender sender = this.connection.getPropertiesFileHelper().
                    createSender(url, host);

            HecChannel channel = new HecChannel(this, sender, this.connection);
            channel.getChannelMetrics().addObserver(this.checkpointManager);
            LOG.debug("Adding channel {}", channel);
            channels.put(channel.getChannelId(), channel);
            //consolidated metrics (i.e. across all channels) are maintained in the checkpointManager

        } catch (MalformedURLException ex) {
            LOG.error(ex.getMessage(), ex);
        }
    }

    //also must not be synchronized
    void removeChannel(String channelId, boolean force) {
        HecChannel c = this.channels.remove(channelId);
        if (c == null) {
            c = this.channelsStaleUrls.remove(channelId);
        }
        /*
    if (c == null) {
      LOG.severe("attempt to cancelEventTrackers unknown channel: " + channelId);
      throw new RuntimeException(
              "attempt to cancelEventTrackers unknown channel: " + channelId);
    }
         */
        if (!force && !c.isEmpty()) {
            LOG.debug(this.checkpointManager.toString());
            throw new HecIllegalStateException(
                    "Attempt to remove non-empty channel: " + channelId + " containing " + c.
                    getUnackedCount() + " unacked payloads",
                    HecIllegalStateException.Type.REMOVE_NON_EMPTY_CHANNEL);

        }

    }

    private synchronized void sendRoundRobin(EventBatch events) throws HecConnectionTimeoutException {
        sendRoundRobin(events, false);
    }

    synchronized void sendRoundRobin(EventBatch events, boolean forced) throws HecConnectionTimeoutException {
        prepareToFindChannel(events, forced);
        long startTime = System.currentTimeMillis();
        int spinCount = 0;
        while (!closed || forced) {
            //note: the channelsSnapshot must be refreshed each time through this loop
            //or newly added channels won't be seen, and eventually you will just have a list
            //consisting of closed channels. Also, it must be a snapshot, not use the live
            //list of channels. Because the channelIdx could wind up pointing past the end
            //of the live list, due to fact that this.removeChannel is not synchronized (and
            //must not be do avoid deadlocks).
            List<HecChannel> channelsSnapshot = new ArrayList<>(this.channels.values());
            if (channelsSnapshot.isEmpty()) {
                continue; //keep going until a channel is added
            }
            if (tryChannelSend(channelsSnapshot, events, forced)) {
                break;
            }
            waitIfSpinCountTooHigh(++spinCount, channelsSnapshot);
            throwExceptionIfTimeout(startTime, events, forced);
        }
    }

    private void prepareToFindChannel(EventBatch events, boolean forced) throws HecIllegalStateException {
        events.incrementNumTries();
        latch = new CountDownLatch(1);
        if (channels.isEmpty()) {
            throw new HecIllegalStateException(
                    "attempt to sendRoundRobin but no channel available.",
                    HecIllegalStateException.Type.LOAD_BALANCER_NO_CHANNELS);
        }
        if (!closed || forced) {
            this.checkpointManager.registeEventBatch(events, forced);
        }
    }

    private void throwExceptionIfTimeout(long start, EventBatch events,
            boolean forced) {
        long timeout = this.getConnection().getBlockingTimeoutMS();
        if (System.currentTimeMillis() - start >= timeout) {
            LOG.warn(
                    PropertyKeys.BLOCKING_TIMEOUT_MS + " exceeded: " + timeout + " ms for id " + events.
                    getId());

            recoverAndThrowException(events, forced,
                    new HecConnectionTimeoutException(
                            "Send timeout exceeded."));
        }
    }

    private boolean tryChannelSend(List<HecChannel> channelsSnapshot,
            EventBatch events, boolean forced) {
        HecChannel tryMe;
        int channelIdx = this.robin++ % channelsSnapshot.size(); //increment modulo number of channels
        tryMe = channelsSnapshot.get(channelIdx);
        try {
            if (tryMe.send(events)) {
                LOG.debug("sent EventBatch:{}  on channel: {}: ", events, tryMe);
                return true;
            }
        } catch (RuntimeException e) {
            recoverAndThrowException(events, forced, e);
        }
        return false;
    }

    private void waitIfSpinCountTooHigh(int spinCount,
            List<HecChannel> channelsSnapshot) {
        if (spinCount % channelsSnapshot.size() == 0) {
            LOG.warn("Waiting for available channel...");
            try {
                latch.await(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                LOG.error(
                        "LoadBalancer caught InterruptedException and resumed. Interruption message was: " + e.
                        getMessage());
            }
            latch = new CountDownLatch(1); //replace the finished countdown latch
        }
    }

    private void recoverAndThrowException(EventBatch events, boolean forced,
            RuntimeException e) {
        //if this is a forced resend by dead channel detector, we *don't* want to cancel the timeout, nor do we need to worry
        //about cleaning up state in AcknowledgementTracker
        if (!forced) {
            events.cancelEventTrackers();
        }
        events.setState(EVENT_POST_FAILURE);
        checkpointManager.cancel(events.getId());
        throw e;
    }

    void wakeUp() {
        if (null != latch) {
            latch.countDown();
        }
    }

    public synchronized void reloadUrls() {
        for (HecChannel c : this.channels.values()) {
            c.close();
        }
        channelsStaleUrls.putAll(channels);
        channels.clear();
        createChannels(discoverer.getAddrs());
    }

    /**
     * @return the channelsPerDestination
     */
    public int getChannelsPerDestination() {
        return channelsPerDestination;
    }

    /**
     * @param channelsPerDestination the channelsPerDestination to set
     */
    public void setChannelsPerDestination(int channelsPerDestination) {
        this.channelsPerDestination = channelsPerDestination;
    }

    /**
     * @return the connection
     */
    public Connection getConnection() {
        return connection;
    }

    /**
     * @return the propertiesFileHelper
     */
    public PropertiesFileHelper getPropertiesFileHelper() {
        return this.connection.getPropertiesFileHelper();
    }

}
