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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Provides methods for preparing  a structured event, as required by the HEC /event endpoint.
 * @author ghendrey
 */
public class EventWithMetadata implements Event {

  private static final ObjectMapper jsonMapper = new ObjectMapper();

  public static final String TIME = "time";
  public static final String HOST = "host";
  public static final String INDEX = "index";
  public static final String SOURCE = "source";
  public static final String SOURCETYPE = "sourcetype";
  public static final String EVENT = "event";
  private String source;
  private String sourceType;
  private String host;
  private String index;
  private long time = -1;
  private final Object event;
  private Comparable id;

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
    EventWithMetadata e = jsonMapper.readValue(eventWithMetadata,
            EventWithMetadata.class);
    e.id = id;
    return e;
  }

  public EventWithMetadata(Object event, Comparable id) {
    if (null == event) {
      throw new IllegalArgumentException("event cannot be null");
    }
    this.event = event;
    this.id = id;
  }

  @Override
  public String toString() {
    Map eventJSON = new LinkedHashMap();

    putIfPresent(eventJSON, TIME, formatTime(time));
    putIfPresent(eventJSON, INDEX, index);
    putIfPresent(eventJSON, HOST, host);
    putIfPresent(eventJSON, SOURCETYPE, sourceType);
    putIfPresent(eventJSON, SOURCE, source);
    eventJSON.put(EVENT, this.event);

    ObjectNode eventNode = (ObjectNode) jsonMapper.valueToTree(eventJSON);
    JsonNodeType type = eventNode.getNodeType();
    try {
      if(type!= JsonNodeType.OBJECT && type!=JsonNodeType.ARRAY && type!=JsonNodeType.POJO) {
        throw new IllegalStateException("Object: " + eventNode);
      }
      return jsonMapper.writeValueAsString(eventNode);
    } catch (IllegalStateException ex) {
      Logger.getLogger(EventWithMetadata.class.getName()).
              log(Level.SEVERE, "Incorrect Event type object " + type, ex);
      throw new RuntimeException(ex.getMessage(), ex);
    } catch (Exception ex) {
      Logger.getLogger(EventWithMetadata.class.getName()).
              log(Level.SEVERE, null, ex);
      throw new RuntimeException(ex.getMessage(), ex);
    }
  }

  private static void putIfPresent(Map collection, String tag,
          String value) {
    if (value != null && !value.isEmpty()) {
      collection.put(tag, value);
    }
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
  public String getSourceType() {
    return sourceType;
  }

  /**
   * @param sourceType the sourceType to set
   */
  public void setSourceType(String sourceType) {
    this.sourceType = sourceType;
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

  @Override
  public boolean isJson() {
    return true;
  }

  /**
   * @return the id
   */
  @Override
  public Comparable getId() {
    return id;
  }
}
