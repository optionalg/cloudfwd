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

import java.util.Map;

/**
 *
 * @author ghendrey
 */
public class EventPostResponseValueObject {
  private Map<String, Object> map;


  EventPostResponseValueObject(Map<String, Object> map) {
    if(!map.containsKey("ackId") || map.get("ackId") == null) {
        throw new IllegalStateException();
    }
    this.map = map;
  }
  
  

  /**
   * @return the ackId
   */
  public Long getAckId() {
    return Long.parseLong(map.get("ackId").toString());
  }


  
}
