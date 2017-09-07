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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author ghendrey
 */
class IndexDiscoverer extends Observable {

  private static final Logger LOG = Logger.getLogger(IndexDiscoverer.class.getName());

  //note, the key is a string representation of the URL. It is critical that the String, and not the URL
  //Object be used as the key. This is because URL implements equals based on comparing the set of
  //InetSocketAddresses resolved. This means that equality for URL changes based on DNS host resolution
  //and would be changing over time
  private final Map<String, List<InetSocketAddress>> mappings;
  private final ConnectionSettings connectionSettings;// = new ConnectionSettings();

  IndexDiscoverer(ConnectionSettings f) {
    this.connectionSettings = f;
    this.mappings = getInetAddressMap(connectionSettings.getUrls(),
        f.isForcedUrlMapToSingleAddr());
  }

  public List<InetSocketAddress> getInetSockAddrs(){
    List<InetSocketAddress> addrs = new ArrayList<>();
    for(List<InetSocketAddress> list:this.mappings.values()){
      addrs.addAll(list);
    }
    return addrs;
  }

  synchronized List<URL> getUrls() {

    List<URL> urls = new ArrayList<>();
    if (connectionSettings.isCloudInstance()) {
      urls.add(connectionSettings.getUrls().get(0));
      return urls;
    }
    for (InetSocketAddress addr : getAddrs()) {
      try {
          InetAddress a = addr.getAddress();
          urls.add(
                  new URL("https://" +
                          addr.getAddress().getHostAddress() +
                          ":" + addr.getPort()));
      } catch (MalformedURLException ex) {
        // log exception an re-throw
        LOG.throwing(IndexDiscoverer.class.getName(), "getUrls", ex);
      }
    }
    return urls;
  }

  synchronized List<InetSocketAddress> getAddrs(){
    List<InetSocketAddress> addrs = new ArrayList<>();
    for (String url : this.mappings.keySet()) {
      addrs.addAll(mappings.get(url));
    }
    return addrs;
  }

  public InetSocketAddress randomlyChooseAddr(){
    List<InetSocketAddress> addrs = getAddrs();
    return addrs.get(new Random(System.currentTimeMillis()).nextInt(addrs.size()));
  }

  /*
  * called by IndexerDiscoveryScheduler
  */
  synchronized void discover(){
    update(getInetAddressMap(connectionSettings.getUrls(),
        connectionSettings.isForcedUrlMapToSingleAddr()), mappings);
  }

  List<Change> update(Map<String, List<InetSocketAddress>> current,
          Map<String, List<InetSocketAddress>> prev) {
    List<Change> changes = new ArrayList<>();
    for (String url : current.keySet()) {
      List<InetSocketAddress> prevSockAddrs = prev.get(url);
      changes.addAll(computeDiff(current.get(url), prevSockAddrs));
    }
    return changes;
  }

  List<Change> computeDiff(List<InetSocketAddress> current,
          List<InetSocketAddress> prev) {
    if (null == prev && null == current) {
      return Collections.EMPTY_LIST;
    }
    if (null == current && null != prev) {
      return asChanges(Collections.EMPTY_LIST, prev);
    }
    if (null == prev && null != current) {
      return asChanges(current, Collections.EMPTY_LIST);
    }
    List<Change> changes = new ArrayList<>();
    List<InetSocketAddress> added = new ArrayList<>(current); //make a copy (.removeAll is mutating)
    added.removeAll(prev);
    List<InetSocketAddress> removed = new ArrayList<>(prev);
    removed.removeAll(current);
    return asChanges(added, removed);
  }

  /**
   * Given a set of URLs of for "protocol://host:port", map each URL string to a
   * set of InetAddresses. FOr instance, urls could be "https://localhost:8088"
   * and "http://localhost:9099". Note that each of these maps to a different
   * set of InetSocketAddresses.
   *
   * @param urls
   * @return
   */
  final static Map<String, List<InetSocketAddress>> getInetAddressMap(
          List<URL> urls, boolean forceSingle) {
    ConcurrentSkipListMap<String, List<InetSocketAddress>> mapping = new ConcurrentSkipListMap<>();
    for (URL url : urls) {
      try {
        String host = url.getHost();

        List<InetAddress> addrs = new ArrayList<>();
        if (forceSingle)
          addrs.add(InetAddress.getByName(host));
        else
          addrs.addAll(Arrays.asList(InetAddress.getAllByName(host)));

        for (InetAddress iaddr : addrs) {
          InetSocketAddress sockAddr = new InetSocketAddress(iaddr, url.
                  getPort());
          mapping.computeIfAbsent(url.toString(), k -> {
            return new ArrayList<>();
          }).add(sockAddr);
        }
      } catch (UnknownHostException ex) {
        Logger.getLogger(IndexDiscoverer.class.getName()).
                log(Level.SEVERE, "Unknown Host: ''{0}''", url.getHost());
        Logger.getLogger(IndexDiscoverer.class.getName()).
                log(Level.SEVERE, null, ex);
      }
    }
    return mapping;
  }

  List<Change> asChanges(List<InetSocketAddress> added,
          List<InetSocketAddress> removed) {
    List<Change> changes = new ArrayList<>();
    for (InetSocketAddress a : added) {
      addChange(changes, new Change(Change.Diff.ADDED, a));
    }
    for (InetSocketAddress a : added) {
      addChange(changes, new Change(Change.Diff.REMOVED, a));
    }
    return changes;
  }

  void addChange(List<Change> changes, Change change){
    changes.add(change);
    setChanged();
    notifyObservers(change);
  }

  public static class Change {

    public static enum Diff {
      ADDED, REMOVED
    }
    private Diff change;
    private InetSocketAddress inetSocketAddress;

    public Change(Diff change, InetSocketAddress inetSocketAddress) {
      this.change = change;
      this.inetSocketAddress = inetSocketAddress;
    }

    @Override
    public String toString() {
      return "NETWORK: Change{" + "change=" + change + ", inetSocketAddress=" + inetSocketAddress + '}';
    }



    /**
     * @return the change
     */
    public Diff getChange() {
      return change;
    }

    /**
     * @param change the change to set
     */
    public void setChange(Diff change) {
      this.change = change;
    }

    /**
     * @return the inetAddress
     */
    public InetSocketAddress getInetAddress() {
      return inetSocketAddress;
    }
  }

}
