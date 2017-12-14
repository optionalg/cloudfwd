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
package com.splunk.cloudfwd.impl.util;

import com.splunk.cloudfwd.ConnectionSettings;
import com.splunk.cloudfwd.HecHealth;
import com.splunk.cloudfwd.LifecycleEvent;
import com.splunk.cloudfwd.error.*;
import com.splunk.cloudfwd.impl.EventBatchImpl;
import com.splunk.cloudfwd.impl.ConnectionImpl;
import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.impl.http.HttpSender;
import java.io.Closeable;
import java.net.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

import static com.splunk.cloudfwd.PropertyKeys.MAX_TOTAL_CHANNELS;
import static com.splunk.cloudfwd.LifecycleEvent.Type.EVENT_POST_FAILED;
import java.util.Collections;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Predicate;

/**
 *
 * @author ghendrey
 */
public class LoadBalancer implements Closeable {
    private final Logger LOG;
    private int channelsPerDestination;
    private final Map<String, HecChannel> channels = new ConcurrentHashMap<>();
    private final Map<String, HecChannel> staleChannels = new ConcurrentHashMap<>();
    private final IndexDiscoverer discoverer;
    private int robin; //incremented (mod channels) to perform round robin
    private final ConnectionImpl connection;
    private volatile boolean quiesced;
    private boolean closed;
    private volatile CountDownLatch latch;
    private volatile boolean interrupted;
    private ScheduledFuture<?> reaperTaskFuture;

    public LoadBalancer(ConnectionImpl c) {
        this.LOG = c.getLogger(LoadBalancer.class.getName());
        this.connection = c;
        this.channelsPerDestination = c.getPropertiesFileHelper().getChannelsPerDestination();
        this.discoverer = new IndexDiscoverer(c.getPropertiesFileHelper(), c);
        createChannels(discoverer.getAddrs());
        //Reaping will randomly remove a channel and replace it with a fresh one every so often. 
        //This insures that channels get spread across indexers, even when we are fronted by an ELB
        setupReaper(); 
    }


    /**
     * Gets the current HecHealth of each channel. This method does not initiate any HTTP traffic.  It just
     * returns whatever each HecChannel's health is at the current instant.
     * @return
     */
    public List<HecHealth> getHealth() {
        final List<HecHealth> h = new ArrayList<>();
        channels.values().forEach(c->h.add(c.getHealth()));
        return h;
    }
    
    public List<HecHealth> getHealthNonBlocking() {
        final List<HecHealth> h = new ArrayList<>();
        channels.values().forEach(c->h.add(c.getHealthNonblocking()));
        return h;
    }    

    public void sendBatch(EventBatchImpl events) throws HecConnectionTimeoutException,
            HecNoValidChannelsException {
        if (null == this.connection.getCallbacks()) {
            throw new HecConnectionStateException(
                    "Connection FutureCallback has not been set.",
                    HecConnectionStateException.Type.CONNECTION_CALLBACK_NOT_SET);
        }
        sendRoundRobin(events);
    }

    @Override
    public synchronized void close() {
        quiesced = true;
        killReaper();
        for (HecChannel c : this.channels.values()) {
            c.close();
        }
        //fixme todo: should we forceClose staleChannels?
        this.closed = true;
    }

    public void closeNow() {
        quiesced = true;
        killReaper();
        for (HecChannel c : this.channels.values()) {
            c.forceClose();
        }
        for (HecChannel c : this.staleChannels.values()) {
            c.forceClose();
        }
        // fail all unacked events. Must close all channels first. If not, there is a possibility that events not yet 
        // in the timeout checker make it into a channel between the time we fail everything in the timeout checker and
        // the time we close the channels
        Collection<EventBatchImpl> unacked = getConnection().getTimeoutChecker().getUnackedEvents();
        unacked.forEach((e)->getConnection().getCallbacks().failed(e, new HecConnectionStateException(
                "Connection closed with unacknowleged events remaining.", HecConnectionStateException.Type.CONNECTION_CLOSED)));
        this.closed = true;
    }

    private synchronized void createChannels(List<InetSocketAddress> addrs) {
        Collections.shuffle(addrs); //we do this so that if more than one connection, and fewer max_total_channels than addrs, that each connection won't choose the same addr
        //add multiple channels for each InetSocketAddress
        for (int i = 0; i < channelsPerDestination; i++) {
            for (InetSocketAddress s : addrs) {
                try {
                    if(addChannel(s, false) == null){
                        break; //reached MAX_TOTAL_CHANNELS
                    }
                } catch (InterruptedException ex) {
                    LOG.warn("ChannelInstantiation interrupted: {}", ex.getMessage());
                }
            }
        }
        waitForPreflight();
    }
    
    private void setupReaper() {
        //One channel will be decomissioned each time the scheduler, below, fires. And there will be an interval of decomMS
        //between each channel that is decomissioned. We need to avoid "storm" of decomissioning many channels at once.
        long decomMs = getPropertiesFileHelper().getChannelDecomMS();
        if (decomMs > 0) {
            this.reaperTaskFuture  = ThreadScheduler.getSharedSchedulerInstance("channel_decom_scheduler").scheduleWithFixedDelay(() -> {
                ArrayList<HecChannel> channels = (ArrayList) this.channels.values();
                if(!channels.isEmpty()){
                    channels.get(0).reapChannel(decomMs); //since channels are in unordered map, item "0" is essentially a random member of the values-set
                }
            }, decomMs, decomMs, TimeUnit.MILLISECONDS); //randomize the channel decommission - so that all channels do not decomission simultaneously.
        }
    }
    
    void killReaper() {
        if (null != reaperTaskFuture && !reaperTaskFuture.isCancelled()) {
            reaperTaskFuture.cancel(true);
        }
    }
    
    private void waitForPreflight() {
        List<HecChannel> channelsList = new ArrayList<>(channels.values()); // get a "snapshot" since decommissioning may kick in 
         //if urls have say, bad protocol, like "flort://host:port" then we cannot instantiate a channel and there is no point in doing 
         //do because the channel instantiated with protocol "floort" is never going to recover until someone changes the protocol. 
        if(channelsList.isEmpty()){
            return;
         }
        
        ExecutorService preflightExecutor = Executors.newFixedThreadPool(channelsList.size());
        try{
            List<HecHealth> healths = new ArrayList<>();
            List<Future<Void>> futures = new ArrayList<>();

            channelsList.forEach((c) -> {
                futures.add(
                    preflightExecutor.submit(()->{
                        c.start();
                        return null;
                    })
                );
                healths.add(c.getHealthNonblocking());
            });
            waitForOnePreflightSuccess(healths, futures, channelsList);
        }finally{
            preflightExecutor.shutdown(); //not shutdownNow credit Sergey Sergeev. Need to allow all the c.start to complete without interruption
        }
    }

    /**
     * Loop until either one preflight check passes, all preflight checks fail, or timeout is reached.
     * If this times out without all preflights completing, load balancer send logic will catch that no
     * channels are available and throw an exception on send.
     */
    private void waitForOnePreflightSuccess(List<HecHealth> healths, List<Future<Void>> futures, 
            List<HecChannel> channelsList) {
        long startMS = System.currentTimeMillis();
        long timeoutMS = getPropertiesFileHelper().getPreFlightTimeoutMS();
        boolean preFlightPassed = false;
        while (!Thread.interrupted()) {
            int numFailed = 0;
            for (int i = 0; i < healths.size(); i++) {
                HecHealth h = healths.get(i);
                if (h.passedPreflight()) {
                    preFlightPassed = true;
                    break;
                }
                if (h.getStatus().getType() == LifecycleEvent.Type.PREFLIGHT_FAILED) {
                    numFailed++;
                }
            }
            if (preFlightPassed || numFailed >= channelsList.size()) {
                break; // one preflight passed or they all failed
            }
            if (System.currentTimeMillis() - startMS >= timeoutMS) {
                // timeout reached
                for (int i = 0; i < channelsList.size(); i++) {
                    channelsList.get(i).preFlightTimeout();
                    futures.get(i).cancel(true); // in case
                }
                break;
            }
            try {
                Thread.sleep(100); // so we don't hog the cpu
            } catch (InterruptedException e) {
                LOG.warn("Sleep interrupted waiting for preflight");
                break;
            }
        }
        
    }

    public HecChannel addChannelFromRandomlyChosenHost(boolean forced) throws InterruptedException {   
        if(!forced && quiesced){
            return null; //don't bother adding channel if we are shutting down, and we're not forcing it
        }
        InetSocketAddress addr = discoverer.randomlyChooseAddr();
        LOG.debug("Adding channel for socket address  {}", addr);
        HecChannel channel = addChannel(addr, true); //this will force the channel to be added, even if we are ac MAX_TOTAL_CHANNELS
        if (channel != null) {
            channel.start();
        }
        return channel;
    }

    private HecChannel addChannel(InetSocketAddress s, boolean force) throws InterruptedException {
        //sometimes we need to force add a channel. Specifically, when we are replacing a reaped channel
        //we must add a new one, before we cancelEventTrackers the old one. If we did not have the force
        //argument, adding the new channel would get ignored if MAX_TOTAL_CHANNELS was set to 1,
        //and then the to-be-reaped channel would also be removed, leaving no channels, and
        //send will be stuck in a spin loop with no channels to send to
        ConnectionSettings settings = this.connection.getSettings();
        if (!force && channels.size() >= settings.getMaxTotalChannels()) {
            LOG.warn(
                    "Can't add channel (" + MAX_TOTAL_CHANNELS + " set to " + settings.
                    getMaxTotalChannels() + ")");
            return null;
        }
        HttpSender sender = (getPropertiesFileHelper()).createSender(s);

        HecChannel channel = new HecChannel(this, sender, this.connection);
        channel.getChannelMetrics().addObserver(this.connection.getCheckpointManager());
        LOG.debug("Adding channel {}", channel);
        channels.put(channel.getChannelId(), channel);
        return channel;
    }

    //also must not be synchronized
    public void removeChannel(String channelId, boolean force) {
        LOG.info("removing from load balancer: channel id {}", channelId);
        HecChannel c = this.channels.remove(channelId);
        if (c == null) {
            c = this.staleChannels.remove(channelId);
        }
        
//        if (c == null) {
//          LOG.error("attempt to cancelEventTrackers unknown channel: " + channelId);
//          throw new RuntimeException(
//                  "attempt to cancelEventTrackers unknown channel: " + channelId);
//        }
         
        if (!force && !c.isEmpty()) {
            LOG.debug(this.connection.getCheckpointManager().toString());
            throw new HecIllegalStateException(
                    "Attempt to remove non-empty channel: " + channelId + " containing " + c.
                    getUnackedCount() + " unacked payloads",
                    HecIllegalStateException.Type.REMOVE_NON_EMPTY_CHANNEL);

        }

    }

    private void sendRoundRobin(EventBatchImpl events) throws HecConnectionTimeoutException,
            HecNoValidChannelsException {
        sendRoundRobin(events, false);
    }

    public boolean sendRoundRobin(EventBatchImpl events, boolean resend)
            throws HecConnectionTimeoutException, HecNoValidChannelsException {
        interrupted = false;
        events.incrementNumTries();
        if (resend && !isResendable(events)) {
            LOG.trace("Not resendable {}", events);
            return false; //bail if this EventBatch has already reached a final state
        }
        preSend(events, resend);
        return spinSend(resend, events);
    }

    private boolean spinSend(boolean resend, EventBatchImpl events) throws HecNoValidChannelsException {
        long startTime = System.currentTimeMillis();
        int spinCount = 0;
        while (true) {
            if (interrupted || (closed && !resend)) {
                return false; 
            }
            //note: the channelsSnapshot must be refreshed each time through this loop
            //or newly added channels won't be seen, and eventually you will just have a list
            //consisting of closed channels. Also, it must be a snapshot, not use the live
            //list of channels. Because the channelIdx could wind up pointing past the end
            //of the live list, due to fact that this.removeChannel is not synchronized (and
            //must not be do avoid deadlocks).
            List<HecChannel> channelsSnapshot = new ArrayList<>(this.channels.
                    values());
            if (channelsSnapshot.isEmpty()) {                
                try {
                    if(closed){
                        throw new HecConnectionStateException("No channels", HecConnectionStateException.Type.NO_HEC_CHANNELS);
                    }
                    //if you don't sleep here, we will be in a hard loop and it locks out threads that are trying to add channels
                    //(This was observed through debugging).
                    LOG.warn("no channels in load balancer");
                    Thread.sleep(100); 
                } catch (InterruptedException ex) {
                    interrupted = true;
                    LOG.warn("LoadBalancer Sleep interrupted.");
                }
            }else{
                if (tryChannelSend(channelsSnapshot, events, resend)) {//attempt to send through a channel (ret's false if channel not available)
                    //the following wait must be done *after* success sending else multithreads can fill the connection and nothing sends
                    //because everyone stuck in perpetual wait
                    //waitWhileFull(startTime, events, closed); //apply backpressure if connection is globally full 
                    break;
                }
            }
            waitIfSpinCountTooHigh(++spinCount, channelsSnapshot, events, resend);
            throwExceptionIfTimeout(startTime, events, resend);
        }
        return true; //return false;
    }

    private void preSend(EventBatchImpl events, boolean forced)
            throws HecIllegalStateException {
        if (channels.isEmpty()) {
            throw new HecIllegalStateException(
                    "attempt to sendRoundRobin but no channel available.",
                    HecIllegalStateException.Type.LOAD_BALANCER_NO_CHANNELS);
        }
        if (!closed || forced) {
            this.connection.getCheckpointManager().registerEventBatch(events, forced);
        }
        if(this.channels.size() > getPropertiesFileHelper().getMaxTotalChannels()){
            LOG.warn("{} exceeded. There are currently: {}",  PropertyKeys.MAX_TOTAL_CHANNELS, channels.size());
        }
    }

    private void throwExceptionIfTimeout(long start, EventBatchImpl events,
            boolean forced) {
        long timeout = this.getConnection().getBlockingTimeoutMS();
        if (System.currentTimeMillis() - start >= timeout) {
            LOG.warn(
                    PropertyKeys.BLOCKING_TIMEOUT_MS + " exceeded: " + timeout + " ms for id " + events.
                    getId());

            recoverAndThrowException(events, forced,
                    new HecConnectionTimeoutException(
                            PropertyKeys.BLOCKING_TIMEOUT_MS + " timeout exceeded on send for EventBatch " +
                                    "with id=" + events.getId() + " Timeout was " + timeout));
        }
    }

    //return true if events has been succesfully sent, or ejected because failed.. A known use case for ejection is when the 
    //EventBatch times out while it is stuck in spin send because there are no available channels. Once the batch times out
    //it must be ejected so we don't send a batch that is already failed due to timeout.
    private boolean tryChannelSend(List<HecChannel> channelsSnapshot,
            EventBatchImpl events, boolean forced) {
        HecChannel tryMe;
        if(events.isFailed()){
            LOG.warn("load balancer ejecting failed event batch {}", events);
            return true;
        }
        int channelIdx = this.robin++ % channelsSnapshot.size(); //increment modulo number of channels
        tryMe = channelsSnapshot.get(channelIdx);
        try {
            if (tryMe.send(events)) {
                LOG.debug("sent EventBatch:{}  on channel={} available={} full={}", events, tryMe, tryMe.isAvailable(), tryMe.isFull());
                return true;
            }else{
                LOG.debug("channel not available, channel={}", tryMe);
                LOG.debug("Skipped channel={} available={} healthy={} full={} quiesced={} closed={}", tryMe, tryMe.isAvailable(), tryMe.isHealthy(), tryMe.isFull(), tryMe.isQuiesced(), tryMe.isClosed());
            }
        } catch (RuntimeException e) {
            recoverAndThrowException(events, forced, e);
        }
        return false;
    }

    private void waitIfSpinCountTooHigh(int spinCount,
            List<HecChannel> channelsSnapshot, EventBatchImpl events, boolean forced) throws HecNoValidChannelsException {
        if (spinCount % channelsSnapshot.size() == 0) {
            try {
                latch = new CountDownLatch(1);
                if (!latch.await(1, TimeUnit.SECONDS)) {
                    LOG.warn(
                            "Round-robin load balancer waited 1 second at spin count {}, channel idx {}, eventBatch {}",
                            spinCount, this.robin % channelsSnapshot.size(), events.getId());
                            //if we had no healthy channels, which is why we are here, it's possible tht we have no
                            //**valid** channels, which means every channel is returning an HecServerErrorResponse
                            //indicating misconfiguration of HEC
                            checkForNoValidChannels(channelsSnapshot, events, forced);
                }
                latch = null;
                //checkForNoValidChannels(channelsSnapshot, events);
            } catch (InterruptedException e) {
                LOG.warn(
                        "LoadBalancer interrupted.");       
                this.interrupted = true;
            }
        }
    }
    
    private void waitWhileFull(long start, EventBatchImpl events,
            boolean forced){
        while(connection.getTimeoutChecker().isFull()){
            try {
                LOG.warn("ConnectionBuffers full ({} bytes). Load Balancer will block...", connection.getTimeoutChecker().getSizeInBytes());                
                latch = new CountDownLatch(1);
                if (!latch.await(100, TimeUnit.MILLISECONDS)) {
                    LOG.warn(
                            "Round-robin load balancer waited 100 ms because connection was full" );
                }
                latch = null;
            } catch (InterruptedException e) {
                LOG.error(
                        "LoadBalancer latch caught InterruptedException and resumed. Interruption message was: " + e.
                        getMessage());
            }
            throwExceptionIfTimeout(start, events, forced);
        }//while        
    }

     /**
     * Returns true if any channel deemed ok by the 'ok' function
     * @param ok The function that returns true if the channel's health is OK
     * @return true if any channels deemed healthy by the 'ok' function
     */
    public boolean anyChannelOK(Predicate<HecHealth> ok){
        List<HecChannel> channelsSnapshot = new ArrayList<>();
        channelsSnapshot.addAll(this.channels.values());
        return anyChannelOK(channelsSnapshot, ok);
    }
    
    /**
     * Pass in a list of channels, and an function which will be used to check the health of a channel
     * @param channelsSnapshot
     * @param ok The function that returns true if the channel's health is OK
     * @return true if all channels deemed healthy by the 'ok' function
     */
    private static boolean anyChannelOK(List<HecChannel> channelsSnapshot, Predicate<HecHealth> ok){
        //First, we run through all the channel's NONBLOCKING get health. This is an optimistic approach wherein if we
        //do find a single not-misconfigured channel then we are good to go
        for(HecChannel c:channelsSnapshot){
            HecHealth health = c.getHealthNonblocking(); 
            if(null != health.getStatus() && ok.test(health)){ 
                return true; //bail, good to go
            }
        }
        //If we are here then we *didn't* find any channel that passed the predicate on the optimistically aquired health
        List<HecHealth> healths = new ArrayList<>();
        for(HecChannel c:channelsSnapshot){ //...so now we need to pessimistically check health
            HecHealth health = c.getHealth(); //this blocks until the health status has been set at  least once
            healths.add(health);
            if(ok.test(health) ){ 
                return true; //bail, good to go
            }
        } 
        return false;
    }
    
    //this method throws HecNoValidChannelsException if *all* the channels are misconfigured (non-200 HEC response)
    private void checkForNoValidChannels(List<HecChannel> channelsSnapshot,
            EventBatchImpl events, boolean forced) throws HecNoValidChannelsException {
        //the 'ok' function determines if the channel health meets certain criterea, in this case 'not misconfigured, and has completedPreflight'
        Predicate<HecHealth> ok = (h)->{
            return !h.isMisconfigured() && h.passedPreflight();
        };
        if(anyChannelOK(channelsSnapshot, ok)){
            return;
        }
        List<HecHealth> healths = new ArrayList<>();
        channelsSnapshot.forEach(c->{healths.add(c.getHealthNonblocking());});
        
        String msg = "No valid channels available due to possible misconfiguration.";
        HecNoValidChannelsException ex = new HecNoValidChannelsException(
            msg, healths);
        LOG.error(msg, ex);
        recoverAndThrowException(events, forced, ex); // removes event trackers        
    }

     private void recoverAndThrowException(EventBatchImpl events, boolean forced,
            RuntimeException e) {
        //if this is a forced resend by dead channel detector, we *don't* want to cancel the timeout, nor do we need to worry
        //about cleaning up state in AcknowledgementTracker
        if (!forced) {
            events.cancelEventTrackers();
        }
        events.setState(EVENT_POST_FAILED);
        this.connection.getCheckpointManager().cancel(events.getId());
        throw e;
    }

    void wakeUp() {
        //we need to take hold a reference to latch in tmp, which won't get nullled between the if block
        //and calling latch.countdown
        CountDownLatch tmp = latch;
        if (null != tmp) {
            tmp.countDown();
        }
    }

    public synchronized void refreshChannels()  {
        discoverer.getAddrs(); //do this now to try to generate any exceptions related to the URL 
        staleChannels.putAll(channels);
        for (HecChannel c : this.channels.values()) {
            try {
                c.closeAndReplace(); //will block while new channel passes preflight or preflight times out. Do this to avoid "storm" of preflight requests on active connection
            } catch (InterruptedException ex) {
                LOG.error("Interrupted trying to close and replace {}", c);
            }
        }
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
    public ConnectionImpl getConnection() {
        return connection;
    }

    /**
     * @return the propertiesFileHelper
     */
    public PropertiesFileHelper getPropertiesFileHelper() {
        return this.connection.getPropertiesFileHelper();
    }

    private boolean isResendable(EventBatchImpl events) {
         final int maxRetries = getPropertiesFileHelper().getMaxRetries();
        if (events.getNumTries() > maxRetries) {
                              String msg = "Tried to send event id=" + events.
                              getId() + " " + events.getNumTries() + " times.  See property " + PropertyKeys.RETRIES;
                      LOG.warn(msg);
                      getConnection().getCallbacks().failed(events,new HecMaxRetriesException(msg));
                      return false;
        }
        if (events.isAcknowledged() || events.isTimedOut(connection.
                getSettings().getAckTimeoutMS())) {
            return false; //do not resend messages that are in a final state 
        }
        events.prepareToResend(); //we are going to resend it,so mark it not yet flushed, cancel its acknowledgement tracker, etc
        return true;
    }

}
