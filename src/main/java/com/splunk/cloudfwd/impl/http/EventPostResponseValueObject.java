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
package com.splunk.cloudfwd.impl.http;

/**
 * if ack's disabled we get back {"text":"Success", "code":0}. If ack's enabled, on success we get back {"ackId":42}
 * @author ghendrey
 */
public class EventPostResponseValueObject {
  private String text; //if ack's disabled we see this field in resp    
  private int code = -1;//...and this field
  private long ackId = -1; //from a normal event post response we just expect to see the ackid

    public EventPostResponseValueObject() {
    }
  
    /**
     * If acks are disabled on server, server will response with {"text":"Success","code":0}"
     * @return
     */
    public boolean isAckIdReceived(){
      return ackId >= 0;
    }
    
    public boolean isAckDisabled(){
        return code==0 && text.equalsIgnoreCase("success") && ackId == -1;
    }
    

/*
  EventPostResponseValueObject(Map<String, Object> map){
    this.map = map;
  }
  */
  
  

//  /**
//   * @return the ackId
//   */
//    
//  public Long getAckId() {
//    return Long.parseLong(map.get("ackId").toString());
//  }

    /**
     * @return the text
     */
    public String getText() {
        return text;
    }

    /**
     * @return the code
     */
    public int getCode() {
        return code;
    }

    /**
     * @return the ackId
     */
    public long getAckId() {
        return ackId;
    }


  
}
