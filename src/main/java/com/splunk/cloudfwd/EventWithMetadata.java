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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.splunk.cloudfwd.impl.ConnectionImpl;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides methods for preparing a structured event, as required by the HEC /event endpoint.
 * @author ghendrey
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class EventWithMetadata implements Event {
  // No access to Connection instance so must use SLF4J logger
  private static final Logger LOG = LoggerFactory.getLogger(EventWithMetadata.class.getName());
  private static final ObjectMapper jsonMapper = new ObjectMapper();

  private String source;
  private String sourcetype;
  private String host;
  private String index;
  @JsonIgnore
  private long time = -1;
  private final Object event;
  @JsonIgnore
  private Comparable id;
  @JsonIgnore
  private byte[] bytes; //for memo-izing the bytes...not part of what gets marshalled to json

  /**
   * Allows caller to provide a HEC /event endpoint JSON document as byte array
   *
   * @param eventWithMetadata
   * @param id
   * @return
   * @throws IOException
   */
  public static EventWithMetadata fromJsonAsBytes(byte[] eventWithMetadata,
          Comparable id) throws IOException {
    //validate by parsing in the eventWithMetadata
    EventWithMetadata e = jsonMapper.readValue(eventWithMetadata,
            EventWithMetadata.class);
    e.id = id;
    return e;
  }

  public EventWithMetadata(Object event, Comparable id) {
    if (null == event) {
      throw new IllegalArgumentException("Event field cannot be null");
    }
    if(event.toString().trim().isEmpty()){
      throw new IllegalArgumentException("Event field cannot be empty");
    }
    this.event = event;
    this.id = id;
  }

  @Override
  public String toString() {
    try {
      return jsonMapper.writeValueAsString(this);
    } catch (Exception ex) {
      LOG.error(ex.getMessage(), ex);
      throw new RuntimeException(ex.getMessage(), ex);
    }
  }
  
  /**
   * WARNING! This method is memo-ized. Any changes to field of this object will not be reflected in getBytes() nor writeTo()
   * subsequent to the first invocation of either.
   * @return
   */
  @Override  
  public byte[] getBytes() {
    try {
      if(null == this.bytes){
        this.bytes = jsonMapper.writeValueAsBytes(this); //MEMO-IZE
      }
      return this.bytes;
    } catch (Exception ex) {
      LOG.error(ex.getMessage(), ex);
      throw new RuntimeException(ex.getMessage(), ex);
    }
  }  
  
    @Override
  public void writeTo(OutputStream out) throws IOException{
    out.write(getBytes());
  }

  public void setTime(long epochMillis) {
    this.time = epochMillis;
  }

  public void setTime(long seconds, long ms) {
    this.time = Instant.ofEpochSecond(seconds).plusMillis(ms).toEpochMilli();
  }

  /**
   * @return the source
   */
  public String getSource() {
    return source;
  }

  /**
   * @param source the source to set
   */
  public void setSource(String source) {
    this.source = source;
  }

  /**
   * @return the sourceType
   */
  public String getSourcetype() {
    return sourcetype;
  }

  /**
   * @param sourceType the sourceType to set
   */
  public void setSourcetype(String sourceType) {
    this.sourcetype = sourceType;
  }

  /**
   * @return the host
   */
  public String getHost() {
    return host;
  }

  /**
   * @param host the host to set
   */
  public void setHost(String host) {
    this.host = host;
  }

  /**
   * @return the index
   */
  public String getIndex() {
    return index;
  }

  /**
   * @param index the index to set
   */
  public void setIndex(String index) {
    this.index = index;
  }

  public long getTime() {
    return time;
  }

  private String formatTime(Long time) {
    if (time >= 0) {
      return String.valueOf(time);
    }
    return null;
  }


  /**
   * @return the id
   */
  @Override
  public Comparable getId() {
    return id;
  }

  @Override
  @JsonIgnore
  public ConnectionImpl.HecEndpoint getTarget() {
    return ConnectionImpl.HecEndpoint.STRUCTURED_EVENTS_ENDPOINT;
  }

  @Override
  @JsonIgnore
  public Type getType() {
    if(event instanceof String){
      return Event.Type.TEXT;
    }else{
      return Event.Type.JSON;
    }
  }

  @Override
  @JsonIgnore
  public InputStream getInputStream() {
    return new ByteArrayInputStream(getBytes());
  }

  @Override
  @JsonIgnore
  public int length() {
    return getBytes().length;
  }

  public Object getEvent() {
    return event;
  }
}
