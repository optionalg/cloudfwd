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
import com.splunk.logging.EventBatch;
import com.splunk.logging.HttpEventCollectorSender;
import java.io.Closeable;
import java.util.Objects;
import java.util.Set;

/**
 *
 * @author ghendrey
 */
public class LoggingChannel implements Comparable, Closeable  {
  private final HttpEventCollectorSender sender;
  //private final SenderFactory logFieldsProvider =  new SenderFactory();
  private static final int FULL=4; //FIXME TODO set to reasonable value, configurable?

  
  public LoggingChannel(HttpEventCollectorSender sender) {
    this.sender = sender;
  }

  public void send(EventBatch events) {
    System.out.println("Sending to channel: " + sender.getChannel());
    sender.sendBatch(events);
  }
  
  
  public void failOver(LoggingChannel recoverer){
   //FIXME TODO
  }
  
  public Set<EventBatch> getUnacknowledgedEvents(){
    return sender.getAckWindow().getUnacknowleldgedEvents();
  }

  boolean betterThan(LoggingChannel other) {
      return this.compareTo(other) > 0;
  }

  /**
   * @return the metrics
   */
  public ChannelMetrics getChannelMetrics() {
    return sender.getChannelMetrics();
  }

  boolean isAvalialable() {
    ChannelMetrics metrics = sender.getChannelMetrics();
    return metrics.getUnacknowledgedCount() < FULL; //FIXME TODO make configurable
           /* LET'S FOR THE MOMENT KEEP THE AVAILABLE CONDITION SUPER SIMPLE AND 
              NOT INCLUDE THESE FANCIER THINGS
            &&
            metrics.getOldestUnackedBirthtime() < System.currentTimeMillis() - 3 * 60 * 1000 //FIXME TODO make configurable oldest unacked less than 3 min old
            &&
            metrics.getMostRecentTimeToSuccess() < 30 * 1000; //FIXME TODO make configurable the most recently acknowledges message took less than 30 seconds
            */

  }

  @Override
  public int compareTo(Object other) {
    if (null == other || ! ((LoggingChannel)other).isAvalialable()) {
      return 1;
    }
    if (this.equals(other)){
      return 0;
    }
    long myBirth = sender.getChannelMetrics().getOldestUnackedBirthtime();
    long otherBirth = ((LoggingChannel)other).getChannelMetrics().getOldestUnackedBirthtime();
    return (int) (myBirth - otherBirth); //channel with youngest unacked message is preferred
  }

  @Override
  public int hashCode() {
    int hash = 3;
    hash = 19 * hash + Objects.hashCode(this.sender.getChannel());
    return hash;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final LoggingChannel other = (LoggingChannel) obj;
    return Objects.equals(this.sender.getChannel(), other.sender.getChannel());
  }

  @Override
  public void close()  {
    this.sender.close();
  }
  
  

}
