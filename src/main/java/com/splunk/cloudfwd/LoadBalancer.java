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

import com.splunk.cloudfwd.http.EventBatch;
import com.splunk.cloudfwd.http.HttpEventCollectorSender;
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
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author ghendrey
 */
public class LoadBalancer implements Observer, Closeable {

  private int channelsPerDestination = 4;
  private static final Logger LOG = Logger.getLogger(LoadBalancer.class.
          getName());
  /* All channels that can be used to send messages. Key is channel ID */
  private final Map<String, LoggingChannel> channels = new ConcurrentSkipListMap<>();
  //private final AtomicBoolean available = new AtomicBoolean(true);
  private PropertiesFileHelper configuredObjectFactory;
  private final ConnectionState connectionState; //consolidate metrics across all channels
  private final IndexDiscoverer discoverer;
  private final IndexDiscoveryScheduler discoveryScheduler;
  private int robin; //incremented (mod channels) to perform round robin
  private final Connection connection;
  private boolean closed;

  LoadBalancer(Connection c) {
    this(c, new Properties());
  }

  LoadBalancer(Connection c,  Properties p) {
    this.connection = c;
    this.configuredObjectFactory = new PropertiesFileHelper(c, p);
    this.channelsPerDestination = this.configuredObjectFactory.
            getChannelsPerDestination();
    this.discoverer = new IndexDiscoverer(c, configuredObjectFactory);
    this.connectionState = new ConnectionState(c);
    this.discoverer.addObserver(this);
    this.discoveryScheduler = new IndexDiscoveryScheduler(this.connection);
  }

  @Override
  /* Called when metric of a channel changes. */
  public synchronized void update(Observable o, Object arg) {
    try {

      System.out.println("LB observed an update of " + arg.getClass().getName());

      if (arg instanceof IndexDiscoverer.Change) {
        updateChannels((IndexDiscoverer.Change) arg);
        return;
      }

      String msg = "Unhandled update from: " + new Object[]{o.
              getClass().getCanonicalName() + " with arg: " + arg.getClass().getName()};
      LOG.severe(msg);
      connection.getCallbacks().failed(null, new Exception(msg));

      throw new RuntimeException("Unhandled update from: " + o.getClass().
              getCanonicalName() + " with arg: " + arg.getClass().getName());
    } catch (Exception e) {
      LOG.severe(e.getMessage());
      connection.getCallbacks().failed(null, e);
    } catch (Throwable t) {
      System.out.println("shit");
    }

  }

  private void updateChannels(IndexDiscoverer.Change change) {
    System.out.println(change);
  }

  public synchronized void sendBatch(EventBatch events)
          throws TimeoutException {
    if(null == this.connection.getCallbacks()){
      throw new IllegalStateException("Connection FutureCallback has not been set.");
    }
    this.connectionState.registerInFlightEvents(events);
    if (channels.isEmpty()) {
      createChannels(discoverer.getAddrs());
    }
    sendRoundRobin(events);
  }

  @Override
  public void close() {
    this.discoveryScheduler.stop();
    synchronized(channels){
      for (LoggingChannel c : this.channels.values()) {
        c.close();
      }
    }
    this.closed = true;
  }
  
  void closeNow() {
    this.discoveryScheduler.stop();
    synchronized(channels){
      for (LoggingChannel c : this.channels.values()) {
        c.forceClose();
      }
    }
    this.closed = true;
  }
  

  public ConnectionState getConnectionState() {
    return this.connectionState;
  }

  private synchronized void createChannels(List<InetSocketAddress> addrs) {
    for (InetSocketAddress s : addrs) {
      //add multiple channels for each InetSocketAddress
      for (int i = 0; i < channelsPerDestination; i++) {
        addChannel(s);
      }
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
      LoggingChannel c = new LoggingChannel(this, sender);
      sender.setChannel(c);
      addChannel(c);     
    } catch (MalformedURLException e) {
      LOG.severe(e.getMessage());
      connection.getCallbacks().failed(null, e);

    }
  }

  //this method must not be synchronized. It will deadlock with threads calling quiesce
  protected void addChannel(LoggingChannel channel) {
    LOG.info("Adding channel " + channel.getChannelId());
    channels.put(channel.getChannelId(), channel);
    //consolidated metrics (i.e. across all channels) are maintained in the connectionState
    channel.getChannelMetrics().addObserver(this.connectionState);

  }

  //also must not be synchronized
  void removeChannel(String channelId, boolean force) {
    LoggingChannel c = this.channels.remove(channelId);
    /*
    if (c == null) {
      LOG.severe("attempt to remove unknown channel: " + channelId);
      throw new RuntimeException(
              "attempt to remove unknown channel: " + channelId);
    }
    */
    if (!force && !c.isEmpty()) {
      String msg = "Attempt to remove non-empty channel: " + channelId + " containing " + c.
              getUnackedCount() + " unacked payloads";
      LOG.severe(msg);
      System.out.println(this.connectionState);
      connection.getCallbacks().failed(null, new Exception(msg));
    }

  }

  private synchronized void sendRoundRobin(EventBatch events) throws TimeoutException {
    try {
      if (channels.isEmpty()) {
        throw new IllegalStateException(
                "attempt to sendRoundRobin but no channel available.");
      }
      LoggingChannel tryMe = null;
      int tryCount = 0;
      //round robin until either A) we find an available channel
      long start = System.currentTimeMillis();
      while (true && !closed) {
        //note: the channelsSnapshot must be refreshed each time through this loop
        //or newly added channels won't be seen, and eventually you will just have a list
        //consisting of closed channels. Also, it must be a snapshot, not use the live
        //list of channels. Becuase the channelIdx could wind up pointing past the end
        //of the live list, due to fact that this.removeChannel is not synchronized (and
        //must not be do avoid deadlocks).
        List<LoggingChannel> channelsSnapshot = new ArrayList<>(this.channels.
                values());
        if (channelsSnapshot.isEmpty()) {
          continue; //keep going until a channel is added
        }
        int channelIdx = this.robin++ % channelsSnapshot.size(); //increment modulo number of channels
        tryMe = channelsSnapshot.get(channelIdx);
        if (tryMe.send(events)) {
          break;
        }
        if(System.currentTimeMillis()-start > Connection.SEND_TIMEOUT){
          System.out.println("TIMEOUT EXCEEDED");
          throw new TimeoutException("Send timeout exceeded.");
        }
      }

    }catch(TimeoutException e){
      throw e;  //we want TimeoutExceptions handled by Caller
    }
    catch (Exception e) {
      LOG.severe("Exception caught in sendRountRobin: " + e.
              getMessage());
      connection.getCallbacks().failed(events, e);
      throw new RuntimeException(e.getMessage(), e);
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
  public Connection getConnection() {
    return connection;
  }
  
}
