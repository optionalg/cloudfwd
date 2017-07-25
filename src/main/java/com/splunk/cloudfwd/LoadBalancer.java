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

import com.splunk.logging.AckLifecycleState;
import com.splunk.logging.EventBatch;
import com.splunk.logging.HttpEventCollectorSender;
import java.io.Closeable;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.Properties;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author ghendrey
 */
public class LoadBalancer implements Observer, Closeable {

  private static final Logger LOG = Logger.getLogger(LoadBalancer.class.
          getName());
  //private final static long TIMEOUT = 60 * 1000; //FIXME TODO make configurable
  /* All channels that can be used to send messages. Key is channel ID */
  private final Map<String, LoggingChannel> channels = new ConcurrentSkipListMap<>();
  //private final AtomicBoolean available = new AtomicBoolean(true);
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
    //this.discoveryScheduler.start(discoverer);
  }

  @Override
  /* Called when metric of a channel changes. */
  public synchronized void update(Observable o, Object arg) {
    try {
      
      System.out.println("LB observed an update of " + arg.getClass().getName());

      /*
      if (arg instanceof AckLifecycleState) {
        System.out.println("updating availability");
        updateAvailability((AckLifecycleState) arg);
        return;
      }
      */

      if (arg instanceof IndexDiscoverer.Change) {
        updateChannels((IndexDiscoverer.Change) arg);
        return;
      }

      Logger.getLogger(getClass().getName()).log(Level.SEVERE,
              "Unhandled update from: {0} with arg: {1}", new Object[]{o.
                getClass().getCanonicalName(), arg.getClass().getName()});
      throw new RuntimeException("Unhandled update from: " + o.getClass().
              getCanonicalName() + " with arg: " + arg.getClass().getName());
    } catch (Exception e) {
      LOG.severe(e.getMessage());
      throw new RuntimeException(e.getMessage(), e);
    }catch(Throwable t){
      System.out.println("shit");
    }

  }

  private synchronized void updateAvailability(AckLifecycleState state) {
    System.out.println("update availability");
    //note LoggingChannel is just a very thin wrapper around the sender. We aren't really "creating" a channel
    //here, as much as wrapping the channel construct around the sender
    LoggingChannel channel = this.channels.get(state.getSender().getChannel());
    if (null == channel) {
      String msg = "unable to find logging channel: " + state.getSender().
              getChannel();
      LOG.log(Level.SEVERE, msg);
      throw new IllegalStateException(msg);
    }

    // synchronized (this) {
    //if channel is available, then the load balancer as a whole is available because we 
    //have at least this one good channel
    System.out.println("doin' it");
    /*
    if (channel.isAvailable()) {
      this.available.set(true);
      System.out.println("NOTIFY ALL");
      notifyAll(); //unblock send()
    }
    */
    System.out.println("LB update complete");
    //}
  }

  private void updateChannels(IndexDiscoverer.Change change) {
    System.out.println(change);
  }

  public synchronized void sendBatch(EventBatch events, Runnable succesCallback)
          throws TimeoutException {
    this.connectionState.setSuccessCallback(events, succesCallback);
    //synchronized (channels) {
    if (channels.isEmpty()) {
      createChannels(discoverer.getAddrs());
    }
    //}
    /*
    while (!available.get()) {
      try {
        long start = System.currentTimeMillis();
        //  synchronized (this) {
        System.out.println("------BLOCKING-----");
        wait(TIMEOUT);
        // }
        if (System.currentTimeMillis() - start <= TIMEOUT) {
          System.out.println("------UNBLOCKED----");
          break; //got an available channel         
        } else {
          System.out.println("------TIMED OUT----");
          throw new TimeoutException(
                  "Unable to find available HEC logging channel in " + TIMEOUT + " ms.");
        }

      } catch (InterruptedException ex) {
        Logger.getLogger(LoadBalancer.class.getName()).log(Level.SEVERE, null,
                ex);
      }
    }//end while
    */
    sendRoundRobin(events);
  }

  @Override
  public synchronized void close() {
    for (LoggingChannel c : this.channels.values()) {
      c.close();
    }
    this.discoveryScheduler.stop();
  }

  public ConnectionState getConnectionState() {
    return this.connectionState;
  }

  private synchronized void createChannels(List<InetSocketAddress> addrs) {
    for (InetSocketAddress s : addrs) {
      addChannel(s);
    }
  }

  void addChannelFromRandomlyChosenHost() {
    InetSocketAddress addr = discoverer.randomlyChooseAddr();
    LOG.log(Level.INFO, "Adding channel to {0}", addr);
    addChannel(addr);
  }

  private void addChannel(InetSocketAddress s) {
    URL url;
    try {
      url = new URL("https://" + s.getHostName() + ":" + s.getPort());
      System.out.println("Trying to add URL: " + url);
      HttpEventCollectorSender sender = this.configuredObjectFactory.
              createSender(url);
      addChannel(new LoggingChannel(this, sender));
    } catch (MalformedURLException ex) {
      Logger.getLogger(LoadBalancer.class.getName()).log(Level.SEVERE, null,
              ex);
    }
  }

  //this method must not be synchronized. It will deadlock with threads calling quiesce
  protected void addChannel(LoggingChannel channel) {
    LOG.info("Adding channel " + channel.getChannelId());
    /* Remember this channel. */
    channels.put(channel.getChannelId(), channel);
    //consolidated metrics (i.e. across all channels) are maintained in the connectionState
    channel.getChannelMetrics().addObserver(this.connectionState);
    //This load balancer also listens to each channelMetric to keep bestChannel fresh
    //channel.getChannelMetrics().addObserver(this);
    /*
    this.available.set(true);
    this.notifyAll();
    */
  }

  //also must not be synchronized
  void removeChannel(String channelId) {
    
      this.channels.remove(channelId);
      /*
      //if we have removed the last channel, then we are no longer available and send must block
      if (channels.isEmpty()) {
        this.available.set(false);
      }*/
    
  }

  private synchronized void sendRoundRobin(EventBatch events) {
    try {
      if (channels.isEmpty()) {
        throw new IllegalStateException(
                "attempt to sendRoundRobin but no channel available.");
      }
      LoggingChannel tryMe = null;
      int tryCount = 0;
      //round robin until either A) we find an available channel

      while(true) {
        //note: the channelsSnapshot must be refreshed each time through this loop
        //or newly added channels won't be seen, and eventually you will just have a list
        //consisting of closed channels. Also, it must be a snapshot, not use the live
        //list of channels. Becuase the channelIdx could wind up pointing past the end
        //of the live list, due to fact that this.removeChannel is not synchronized (and
        //must not be do avoid deadlocks).
        List<LoggingChannel> channelsSnapshot = new ArrayList<>(this.channels.values());
        int channelIdx = this.robin++ % channelsSnapshot.size(); //increment modulo number of channels
        tryMe = channelsSnapshot.get(channelIdx);
        if(tryMe.send(events)){
          break;
        }else{
          Thread.sleep(20);
        }
      } 


    } catch (Exception e) {
      LOG.severe("Exception caught in sendRountRobin: " + e.getMessage());
      throw new RuntimeException(e.getMessage(), e);
    }

  }

}
