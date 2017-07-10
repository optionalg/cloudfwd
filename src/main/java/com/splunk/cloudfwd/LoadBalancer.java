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

import com.splunk.logging.ChannelMetrics;
import com.splunk.logging.HttpEventCollectorSender;
import java.util.ArrayList;
import java.util.List;
import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author ghendrey
 */
public class LoadBalancer extends Observable implements Observer {

  private final static long TIMEOUT = 60 * 1000; //FIXME TODO make configurable
  private final List<LoggingChannel> channels = new ArrayList<>();
  private LoggingChannel bestChannel;
  private final AtomicBoolean available = new AtomicBoolean(true);
  
  public synchronized void addChannel(LoggingChannel channel){
    channels.add(channel);
    channel.getMetrics().addObserver(this);
  }

  @Override
  public void update(Observable o, Object arg) {
    HttpEventCollectorSender sender = (HttpEventCollectorSender) arg;
    LoggingChannel channel = new LoggingChannel(sender);
    synchronized (this) {
      if (channel.betterThan(bestChannel.getMetrics())) {
        bestChannel = channel;
      }
      //if best channel is not available set the AtomicBoolean to false
      //which will cause send() to block until send times out, or until this
      //update method is called again, and unblocks send() because the best
      //channel became available
      this.available.set(bestChannel.isAvalialable());
      if (this.available.get()) {
        notifyAll(); //unblock send()
      }

    }

  }

  public void send(String msg) throws TimeoutException{
    while (!available.get()) {
      try {
        long start = System.currentTimeMillis();
        synchronized (this) {
          wait(TIMEOUT);
        }
        if(start - System.currentTimeMillis() > TIMEOUT){
          throw new TimeoutException("Unable to find available HEC logging channel in " + TIMEOUT + " ms.");         
        }
      } catch (InterruptedException ex) {
        Logger.getLogger(LoadBalancer.class.getName()).log(Level.SEVERE, null,
                ex);
        continue;
      }
      bestChannel.send(msg);
      return;
    }
  }

}
