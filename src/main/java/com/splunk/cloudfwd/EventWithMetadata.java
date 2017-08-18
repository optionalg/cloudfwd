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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.UnsupportedEncodingException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author ghendrey
 */
public class EventWithMetadata extends RawEvent {

  public static final String EventsEndpointTimeFieldname = "time";
  public static final String EventsEndpointHostFieldname = "host";
  public static final String EventsEndpointIndexFieldname = "index";
  public static final String EventsEndpointSourceFieldname = "source";
  public static final String EventsEndpointSourcetypeFieldname = "sourcetype";
  private String source;
  private String sourceType;
  private String host;
  private String index;
  long time;
  private static ObjectMapper jsonMapper;

  public EventWithMetadata(byte[] event) {
    super(event);
  }

  @Override
  public byte[] getBytes() {
    Map event = new LinkedHashMap();

    putIfPresent(event, EventsEndpointTimeFieldname, String.format(Locale.US,
            "%.3f", getTime()));

    ObjectNode eventNode = (ObjectNode) jsonMapper.valueToTree(event);
    try {
      return jsonMapper.writeValueAsString(eventNode).getBytes("UTF-8");
    } catch (Exception e) {
      Logger.getLogger(EventWithMetadata.class.getName()).
              log(Level.SEVERE, null, e);
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  @SuppressWarnings("unchecked")
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

}
