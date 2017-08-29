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


/**
 * Provides various static methods for obtaining a RawEvent.
 * @author ghendrey
 */
public class RawEvent implements Event{
  private static final ObjectMapper jsonMapper = new ObjectMapper();
  
  final String string;
  private final boolean json;
  private Comparable id;
  
  /**
   * Convenience method that will handle either bytes of a JSON object, or bytes of a UTF-8 String.
   * The bytes are first parsed as a json object. If that fails, the bytes are parsed into a String assuming
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
      e =  new RawEvent(new String(jsonOrText, "UTF-8"), id, false);
    }
    e.id = id;
    return e;
  }
  
  public static RawEvent fromJsonAsBytes(byte[] jsonBytes, Comparable id) throws IOException{
    JsonNode node = jsonMapper.readTree(jsonBytes);
    JsonNodeType type = node.getNodeType();
    if(type!=JsonNodeType.OBJECT && type!=JsonNodeType.ARRAY && type!=JsonNodeType.POJO) {
      throw new IllegalStateException("Incorrect event type object: " + type);
    }
    return new RawEvent(jsonMapper.readTree(jsonBytes).toString(), id,  true);
  }
  
  
  public static RawEvent fromObject(Object o, Comparable id) throws IOException{
    return new RawEvent(jsonMapper.writeValueAsString(o), id,  true);
  }
    
  public static RawEvent fromText(String text, Comparable id){
    return new RawEvent(text, id, false);
  }
  
  private RawEvent(String eventString, Comparable id,  boolean json){
    if(eventString.endsWith("\n")){
      this.string = eventString;
    }else{
      this.string = eventString + "\n"; //insure event ends in line break
    }
    this.json = json;
    this.id = id;
  }
  

  @Override
  public String toString() {
    return string;
  }

  @Override
  public boolean isJson() {
    return json;
  }

  @Override
  public Comparable getId() {
    return id;
  }
  
  
}
