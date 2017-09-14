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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Provides various static methods for obtaining a RawEvent. If content is JSON, it is validated. A newline is appended if
 * nothing is present. A RawEvent should be sent to the /raw HEC events endpoint. The event is 'raw' in the sense that includes
 * no enclosing JSON envelope. 
 * @author ghendrey
 */
public class RawEvent implements Event{
  private static final Logger LOG = LoggerFactory.getLogger(RawEvent.class.getName());
  private static final ObjectMapper jsonMapper = new ObjectMapper();
  
  final byte[] bytes;
  private Comparable id;
  private final Event.Type type;
  
  /**
   * Convenience method that will handle either bytes of a JSON object or bytes of a UTF-8 string.
   * The bytes are first parsed as a JSON object. If that fails, the bytes are parsed into a string assuming
   * UTF-8 encoding in the bytes.
   * @param jsonOrText
   * @param id
   * @return
   * @throws UnsupportedEncodingException
   */
  public static RawEvent fromJsonOrUTF8StringAsBytes(byte[] jsonOrText, Comparable id) throws UnsupportedEncodingException{
    RawEvent e;
    try{
        e = fromJsonAsBytes(jsonOrText, id);
    } catch (IOException ex) {//failed to parse as json, so treat as bytes
      e =  new RawEvent(new String(jsonOrText, "UTF-8").getBytes(), id, Event.Type.TEXT);
    }
    return e;
  }
  
  public static RawEvent fromJsonAsBytes(byte[] jsonBytes, Comparable id) throws IOException{
    JsonNode node = jsonMapper.readTree(jsonBytes);
    JsonNodeType type = node.getNodeType();
    if(type!=JsonNodeType.OBJECT && type!=JsonNodeType.ARRAY && type!=JsonNodeType.POJO) {
      throw new HecIllegalStateException(
              "Incorrect event type object: " + type,
              HecIllegalStateException.Type.INCORRECT_EVENT_TYPE_OBJECT);
    }

    return new RawEvent(jsonBytes, id,  Event.Type.JSON);
  }
  
  
  public static RawEvent fromObject(Object o, Comparable id) throws IOException{
    return new RawEvent(jsonMapper.writeValueAsBytes(o), id,  Event.Type.JSON);
  }
    
  public static RawEvent fromText(String text, Comparable id){
    try {
      return new RawEvent(text.getBytes("UTF-8"), id, Event.Type.TEXT);
    } catch (UnsupportedEncodingException ex) {
      LOG.error(ex.getMessage(), ex);
      throw new RuntimeException(ex.getMessage(), ex);
    }
  }
  
  private RawEvent(byte[] bytes, Comparable id,  Event.Type type){
   
    if(endsWith(bytes, (byte)'\n')){
      this.bytes = bytes;
    }else{
      this.bytes = ArrayUtils.add(bytes, (byte)'\n');
    };
    this.id = id;
    this.type = type;
  }
  
  private static boolean endsWith(byte[] a, byte b){
    return a[a.length-1] == b;
  }
  

  @Override
  public Comparable getId() {
    return id;
  }

  @Override
  public byte[] getBytes() {
    return bytes;
  }
  
  @Override
  public String toString(){
    try {
      return new String(bytes, "UTF-8");
    } catch (UnsupportedEncodingException ex) {
      LOG.error(ex.getMessage(), ex);
      throw new RuntimeException(ex.getMessage(), ex);
    }
  }

  @Override
  public void writeTo(OutputStream out) throws IOException {
    out.write(bytes);
  }

  @Override
  public Connection.HecEndpoint getTarget() {
    return Connection.HecEndpoint.RAW_EVENTS_ENDPOINT;
  }

  @Override
  public Type getType() {
    return type;
  }

  @Override
  public InputStream getInputStream() {
    return new ByteArrayInputStream(bytes);
  }

  @Override
  public int length() {
    if(null == bytes){
      return 0;
    }
    return bytes.length;
  }
  
  
}
