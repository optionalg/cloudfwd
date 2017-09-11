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

import com.splunk.cloudfwd.http.HecIOManager;
import com.splunk.cloudfwd.util.EventTracker;
import com.splunk.cloudfwd.util.HecChannel;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.SequenceInputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import org.apache.http.HttpEntity;
import org.apache.http.entity.AbstractHttpEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author ghendrey
 */
public class EventBatch  implements IEventBatch {

  protected static final Logger LOG = LoggerFactory.getLogger(EventBatch.class.getName());
  protected Comparable id; //will be set to the id of the last (most recent) Event added to the batch
  protected Long ackId; //Will be null until we receive ackId for this batch from HEC
  protected boolean flushed = false;
  protected boolean acknowledged;
  private final long creationTime = System.currentTimeMillis();
  protected int numEvents;
  protected int numTries; //events are resent by DeadChannelDetector
  protected int length;
  protected List<Event> events = new ArrayList<>();
  protected Connection.HecEndpoint knownTarget;
  protected Event.Type knownType;
  protected List<EventTracker> trackers = new ArrayList<>();
  private HecChannel hecChannel;

  public EventBatch() {
  }

  @Override
  public synchronized void prepareToResend() {
    this.flushed = false;
    this.acknowledged = false;
    this.ackId = null;
  }

  @Override
  public boolean isTimedOut(long timeout) {
    long flightTime = System.currentTimeMillis() - creationTime;
    LOG.debug("Flight time " + flightTime);
    return flightTime >= timeout;
  }

  @Override
  public synchronized void add(Event event) {
    if (flushed) {
      throw new IllegalStateException(
              "Events cannot be added to a flushed EventBatch");
    }
    if (null != knownTarget && knownTarget != event.getTarget()) { //and it's intended endpoint target doesn't match
      throw new HecIllegalStateException(
              "Illegal attempt to add event with getTarget()=" + event.
              getTarget()
              + " to EventBatch containing Event with getTarget()=" + knownTarget,
              HecIllegalStateException.Type.MIXED_BATCH);
    }
    if (event.getType() != Event.Type.UNKNOWN) {
      knownTarget = event.getTarget();
      knownType = event.getType();
    }
    this.id = event.getId();
    this.length += event.length();
    this.events.add(event);

  }

  protected synchronized boolean isFlushable(int charBufferLen) {
    //technically the serialized size that we compate to maxEventsBatchSize should take into account
    //the character encoding. it's very difficult to compute statically. We use the stringBuilder length
    //which is a character count. Will be same as serialized size only for single-byte encodings like
    //US-ASCII of ISO-8859-1
    return !flushed && (getLength() > charBufferLen);
  }

  @Override
  public synchronized void post(HecIOManager ioManager) {
    if (!this.flushed && getLength() > 0) {
      //endpoints are either real (via the Sender) or simulated
      ioManager.postEvents(this);
      flushed = true;
      //numTries++;
    }
  }
  
  public void incrementNumTries(){
    numTries++;
  }

  @Override
  public int getNumEvents() {
    return events.size();
  }

  /**
   * @return the id
   */
  @Override
  public Comparable getId() {
    return id;
  }

  /**
   * @return the ackId
   */
  @Override
  public Long getAckId() {
    return ackId;
  }

  /**
   * @param ackId the ackId to set
   */
  @Override
  public void setAckId(Long ackId) {
    this.ackId = ackId;
  }

  /**
   * @return the acknowledged
   */
  @Override
  public boolean isAcknowledged() {
    return acknowledged;
  }

  /**
   * @param acknowledged the acknowledged to set
   */
  @Override
  public void setAcknowledged(boolean acknowledged) {
    this.acknowledged = acknowledged;
  }

  /**
   * @return the flushed
   */
  @Override
  public boolean isFlushed() {
    return flushed;
  }

  /**
   * @return the numTries
   */
  @Override
  public int getNumTries() {
    return numTries;
  }

  @Override
  public int getLength() {
    return length;
  }

  @Override
  public HttpEntity getEntity() {
    AbstractHttpEntity e = new HttpEventBatchEntity();
    if (null == knownTarget) {
      throw new IllegalStateException(
              "getEntity cannot be called until post() has been called.");
    }
    if (knownTarget == Connection.HecEndpoint.STRUCTURED_EVENTS_ENDPOINT) {
      e.setContentType(
              "application/json; profile=urn:splunk:event:1.0; charset=utf-8");
    } else if (knownTarget == Connection.HecEndpoint.RAW_EVENTS_ENDPOINT) {
      if (null != knownType) {
        if (knownType == Event.Type.JSON) {
          e.setContentType(
                  "application/json; profile=urn:splunk:event:1.0; charset=utf-8");
        } else {
          e.setContentType(
                  "text/plain; profile=urn:splunk:event:1.0; charset=utf-8");
        }
      } else { //mixed content. Best to set content type to text/plain
        e.setContentType(
                "text/plain; profile=urn:splunk:event:1.0; charset=utf-8");
      }
    }
    return e;
  }
    
  public void checkCompatibility(Connection.HecEndpoint target) throws HecIllegalStateException {

    if (knownTarget != null) {
      if (knownTarget != target) {
        throw new HecIllegalStateException(
                "EventBatch contained  events wih getTarget()=" + knownTarget +
                        " which is incompatible with HEC endpoint  " + target,
                HecIllegalStateException.Type.INVALID_EVENTS_FOR_ENDPOINT);
      }
    } else {
      knownTarget = target; //this can help us infer the content type as application/json when destined for /events
    }

  }

  @Override
  public String toString() {
    return "EventBatch{" + "id=" + id + ", ackId=" + ackId + ", acknowledged=" + acknowledged + ", numTries=" +numTries +'}';
  }
  
  public void cancelEventTrackers(){
    trackers.forEach(t->{
      t.cancel(this);
    });
  }

  public void registerEventTracker(EventTracker t){
    trackers.add(t);
  }

  /**
   * @return the creationTime
   */
  public long getCreationTime() {
    return creationTime;
  }

  /**
   * @return the hecChannel
   */
  public HecChannel getHecChannel() {
    return hecChannel;
  }

  /**
   * @param hecChannel the hecChannel to set
   */
  public void setHecChannel(HecChannel hecChannel) {
    this.hecChannel = hecChannel;
  }

  private class HttpEventBatchEntity extends AbstractHttpEntity {

    @Override
    public boolean isRepeatable() {
      return true;
    }

    @Override
    public long getContentLength() {
      return getLength();
    }

    @Override
    public InputStream getContent() throws IOException, UnsupportedOperationException {
      return new SequenceInputStream(new Enumeration<InputStream>(){
        int idx=-1;
        @Override
        public boolean hasMoreElements() {
          return !events.isEmpty() && (idx+1) < events.size();
        }

        @Override
        public InputStream nextElement() {
          return events.get(++idx).getInputStream();
        }
      
      });
    }

    @Override
    public void writeTo(OutputStream outstream) throws IOException {
      for (Event e : events) {
        e.writeTo(outstream);
      }

    }

    @Override
    public boolean isStreaming() {
      return false;
    }

  }

}
