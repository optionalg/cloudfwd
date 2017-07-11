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
import java.util.Objects;

/**
 *
 * @author ghendrey
 */
public class LoggingChannel implements Comparable  {
  private final HttpEventCollectorSender sender;
  private final SenderFactory logFieldsProvider =  new SenderFactory();

  
  public LoggingChannel(HttpEventCollectorSender sender) {
    this.sender = sender;
  }

  public void send(String msg) {
    String providedLogger = this.logFieldsProvider.getLogger();
    String logger = providedLogger != null ? providedLogger : "hec.channel." + sender.
            getChannel();
    sender.send(logFieldsProvider.getSeverity(),
            msg,
            logger,
            Thread.currentThread().getName(), null, null, null);
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
    return metrics.getUnacknowledgedCount() > 100
            || //100 outstanding aks
            metrics.getOldestUnackedBirthtime() > System.currentTimeMillis() - 3 * 60 * 1000
            || //or any outstanding ack older than 3 min
            metrics.getMostRecentTimeToSuccess() < 30 * 1000; //or the most recently acknowledges message took longer than 30 seconds

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
  
  

}
