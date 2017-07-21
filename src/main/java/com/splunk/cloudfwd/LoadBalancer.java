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
package com.splunk.cloudfwd;

import com.splunk.logging.EventBatch;
import com.splunk.logging.HttpEventCollectorSender;
import java.io.Closeable;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Observable;
import java.util.Observer;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author ghendrey
 */
public class LoadBalancer implements Observer, Closeable {

  private final static long TIMEOUT = 60 * 1000; //FIXME TODO make configurable
  /* All channels that can be used to send messages. */
  private final List<LoggingChannel> channels = new ArrayList<>();
  /* The best channel to send a message to, according to the metrics */
  private LoggingChannel bestChannel;
  private final AtomicBoolean available = new AtomicBoolean(true);
  private ConfiguredObjectFactory configuredObjectFactory;
  private final ConnectionState connectionState = new ConnectionState(); //consolidate metrics across all channels
  private final IndexDiscoverer discoverer;
  private final IndexDiscoveryScheduler discoveryScheduler = new IndexDiscoveryScheduler();
  private int robin; //incremented (mod channels) to perform round robin

  public LoadBalancer() {
    this(new Properties());
  }

  public LoadBalancer(Properties p) {
    this.configuredObjectFactory = new ConfiguredObjectFactory(p);
    this.discoverer = new IndexDiscoverer(configuredObjectFactory);
    this.discoverer.addObserver(this);
    this.discoveryScheduler.start(discoverer);
  }

  /* Registers a new channel with the load balancer.*/
  protected synchronized void addChannel(LoggingChannel channel) {
    /* Remember this channel. */
    channels.add(channel);
    //consolidated metrics (i.e. across all channels) are maintained in the connectionState
    channel.getChannelMetrics().addObserver(this.connectionState);
    //This load balancer also listens to each channelMetric to keep bestChannel fresh
    channel.getChannelMetrics().addObserver(this);
    /*Give this newly added channel a chance to be the best channel immediately */
    if (channel.betterThan(bestChannel)) {
      bestChannel = channel;
    }
  }

  protected synchronized void removeChannel(LoggingChannel channel) {
    /* Remember this channel. */
    channels.add(channel);
    //consolidated metrics (i.e. across all channels) are maintained in the connectionState
    channel.getChannelMetrics().addObserver(this.connectionState);
    //This load balancer also listens to each channelMetric to keep bestChannel fresh
    channel.getChannelMetrics().addObserver(this);
    /*Give this newly added channel a chance to be the best channel immediately */
    if (channel.betterThan(bestChannel)) {
      bestChannel = channel;
    }
  }

  @Override
  /* Called when metric of a channel changes. */
  public void update(Observable o, Object arg) {
    if (arg instanceof HttpEventCollectorSender) {
      updateBestChannel((HttpEventCollectorSender) arg);
      return;
    }

    if (arg instanceof IndexDiscoverer.Change) {
      updateChannels((IndexDiscoverer.Change) arg);
    }

  }

  private void updateBestChannel(HttpEventCollectorSender sender) {
    LoggingChannel channel = new LoggingChannel(sender);
    synchronized (this) {

      //if channel is available, then the load balancer as a whole is available because we 
      //have at least this one good channel
      if(channel.isAvalialable()){
        this.available.set(true);
        notifyAll(); //unblock send()
      }
    }
  }

  private void updateChannels(IndexDiscoverer.Change change) {
    System.out.println(change);
  }

  public void sendBatch(EventBatch events, Runnable succesCallback) throws TimeoutException {
    this.connectionState.setSuccessCallback(events, succesCallback);
    synchronized (channels) {
      if (channels.isEmpty()) {
        addChannels(discoverer.getAddrs());
      }
    }
    while (!available.get()) {
      try {
        long start = System.currentTimeMillis();
        synchronized (this) {
          wait(TIMEOUT);
        }
        if (start - System.currentTimeMillis() <= TIMEOUT) {
          break; //got an available channel
        } else {
          throw new TimeoutException(
                  "Unable to find available HEC logging channel in " + TIMEOUT + " ms.");
        }
      } catch (InterruptedException ex) {
        Logger.getLogger(LoadBalancer.class.getName()).log(Level.SEVERE, null,
                ex);
      }
    }//end while
    sendRoundRobin(events);
  }

  @Override
  public void close() {
    for (LoggingChannel c : this.channels) {
      c.close();
    }
    this.discoveryScheduler.stop();
  }

  public ConnectionState getConnectionState() {
    return this.connectionState;
  }

  private void addChannels(List<InetSocketAddress> addrs) {
    for (InetSocketAddress s : addrs) {
      URL url;
      try {
        url = new URL("https://" + s.getHostName() + ":" + s.getPort());
        HttpEventCollectorSender sender = this.configuredObjectFactory.
                createSender(url);
        addChannel(new LoggingChannel(sender));
      } catch (MalformedURLException ex) {
        Logger.getLogger(LoadBalancer.class.getName()).log(Level.SEVERE, null,
                ex);
      }
    }
  }

  private synchronized void sendRoundRobin(EventBatch events) {
    LoggingChannel tryMe = null;
    int tryCount = 0;
    //round robin until either A) we find an available channel or B)we tried all channels
    do{
      int channelIdx = this.robin++%this.channels.size(); //increment modulo number of channels
      tryMe = channels.get(channelIdx);
    }while(!tryMe.isAvalialable() && tryCount++ < channels.size());
   
    //if we are here, and tryMe is not available, then NONE of the channels was available
    //and so we should set the entire loadbalancer as unavailable and cause the client to block
    this.available.set(tryMe.isAvalialable());
    tryMe.send(events);
   
    
  }

}
