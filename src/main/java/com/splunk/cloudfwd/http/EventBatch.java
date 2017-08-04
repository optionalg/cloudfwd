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
package com.splunk.cloudfwd.http;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

/**
 *
 * @author ghendrey
 */
public class EventBatch implements SerializedEventProducer {

  private static final Logger LOG = Logger.getLogger(EventBatch.class.getName());
  private static AtomicLong batchIdGenerator = new AtomicLong(0);
  private String id = String.format("%019d", batchIdGenerator.incrementAndGet());//must generate this batch's ID before posting events, since it's string and strings compare lexicographically we should zero pad to 19 digits (max long value)
  private Long ackId; //Will be null until we receive ackId for this batch from HEC
  private Map<String, String> metadata = new HashMap<>();
  //private final TimerTask flushTask = new ScheduledFlush();
  private final List<HttpEventCollectorEvent> eventsBatch = new ArrayList();
  private HttpEventCollectorSender sender;
  private final StringBuilder stringBuilder = new StringBuilder();
  private boolean flushed = false;
  private boolean acknowledged;
  public enum Endpoint {
    event, raw
  }
  public enum Eventtype {
    blob, json
  }

  private Endpoint endpoint;
  private Eventtype eventtype;

  public EventBatch(Endpoint endpoint, Eventtype eventtype) {
    this.sender = null;
    this.endpoint = endpoint;
    this.eventtype = eventtype;
  }

  EventBatch(HttpEventCollectorSender sender, long maxEventsBatchCount,
          long maxEventsBatchSize,
          long flushInterval, Map<String, String> metadata, Timer timer) {
    this.sender = sender;
    // when size configuration setting is missing it's treated as "infinity",
    // i.e., any value is accepted.
    if (maxEventsBatchCount == 0 && maxEventsBatchSize > 0) {
      maxEventsBatchCount = Long.MAX_VALUE;
    } else if (maxEventsBatchSize == 0 && maxEventsBatchCount > 0) {
      maxEventsBatchSize = Long.MAX_VALUE;
    }
    this.metadata = metadata;
  }
  /*
  public void setSimulatedEndpoints(Endpoints endpoints){
    this.simulatedEndpoints = endpoints;
  }
  */

  public synchronized void add(HttpEventCollectorEvent event) {
    if (flushed) {
      throw new IllegalStateException(
              "Events cannot be added to a flushed EventBatch");
    }
    /*
    if (null == this.metadata) {
      throw new RuntimeException("Metadata not set for events");
    }
     */

    eventsBatch.add(event);
    if (this.endpoint == Endpoint.event) {
      stringBuilder.append(event.toEventEndpointString(metadata));
    } else {
      stringBuilder.append(event.toRawEndpointString());
    }
  }

  protected synchronized boolean isFlushable() {
    //technically the serialized size that we compate to maxEventsBatchSize should take into account
    //the character encoding. it's very difficult to compute statically. We use the stringBuilder length
    //which is a character count. Will be same as serialized size only for single-byte encodings like
    //US-ASCII of ISO-8859-1    
    return !flushed && (serializedCharCount() > 0);
  }

  protected synchronized void flush() {
    if (!this.flushed && this.stringBuilder.length() > 0) {
      //endpoints are either real (via the Sender) or simulated 
      this.sender.getAckManager().postEvents(this);
      flushed = true;
    }
  }

  /**
   * Close events sender
   */
  public synchronized void close() {
    //send any pending events, regardless of how many or how big 
    flush();
  }

  @Override
  public String toString() {
    if (this.endpoint == Endpoint.raw) {
      if (this.eventtype == Eventtype.json) {
        List<String> myList = new ArrayList<String>(
                Arrays.asList(this.stringBuilder.toString().split(",")));
        return myList.toString();
      }
    }
    return this.stringBuilder.toString();
  }

  @Override
  public void setEventMetadata(Map<String, String> metadata) {
    this.metadata = metadata;
  }

  int serializedCharCount() {
    return stringBuilder.length();
  }

  int getNumEvents() {
    return eventsBatch.size();
  }

  public int size() {
    return eventsBatch.size();
  }

  public HttpEventCollectorEvent get(int idx) {
    return this.eventsBatch.get(idx);
  }

  public List<HttpEventCollectorEvent> getEvents() {
    return this.eventsBatch;
  }


  /**
   * @return the metadata
   */
  public Map<String, String> getMetadata() {
    return metadata;
  }

  /**
   * @return the id
   */
  public String getId() {
    return id;
  }

  /**
   * @return the ackId
   */
  public Long getAckId() {
    return ackId;
  }

  /**
   * @param ackId the ackId to set
   */
  public void setAckId(Long ackId) {
    this.ackId = ackId;
  }

  void setSender(HttpEventCollectorSender sender) {
    if (null != this.sender) {
      String msg = "attempt to change the value of sender. Channel was " + this.sender.
              getChannel() + ", and attempt to change to " + sender.getChannel();
      LOG.severe(msg);
      throw new IllegalStateException(msg);
    }
    this.sender = sender;
  }

  /**
   * @return the acknowledged
   */
  public boolean isAcknowledged() {
    return acknowledged;
  }

  /**
   * @param acknowledged the acknowledged to set
   */
  public void setAcknowledged(boolean acknowledged) {
    this.acknowledged = acknowledged;
  }

  /**
   * @return the flushed
   */
  public boolean isFlushed() {
    return flushed;
  }

  /**
   * @return the sender
   */
  public HttpEventCollectorSender getSender() {
    return sender;
  }

  public Enum<Endpoint> getEndpoint() {return endpoint;}

  private class ScheduledFlush extends TimerTask {

    @Override
    public void run() {
      flush();
    }

  }

}