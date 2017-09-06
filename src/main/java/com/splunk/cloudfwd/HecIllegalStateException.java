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

/**
 * Thrown if duplicate ack-id is received on a given HEC channel. This can indicate failure of a sticky load
 * balancer to provide stickiness.
 * @author ghendrey
 */
public class HecIllegalStateException extends IllegalStateException{
  public enum Type{STICKY_SESSION_VIOLATION, INVALID_EVENTS_FOR_ENDPOINT, MIXED_BATCH}
  private final Type type;

  /**
   * @return the type
   */
  public Type getType() {
    return type;
  }

  public HecIllegalStateException(String s, Type t) {
    super(s);
    this.type = t;
  }
  
  
}
