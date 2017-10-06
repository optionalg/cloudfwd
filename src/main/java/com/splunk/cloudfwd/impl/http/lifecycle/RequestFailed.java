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
package com.splunk.cloudfwd.impl.http.lifecycle;

import com.splunk.cloudfwd.LifecycleEvent;

/**
 *
 * @author ghendrey
 */
public class RequestFailed extends LifecycleEvent implements Failure{

  private final Exception exception;
  
  public RequestFailed(Type type, Exception e) {
    super(type);
    this.exception = e; 
  }

    @Override
    public String toString() {
        return "RequestFailed{"+super.toString() + " exception=" + exception + '}';
    }
  
  

  /**
   * @return the exception
   */
  public Exception getException() {
    return exception;
  }
  
  public boolean isOK(){
      return false;
  }
  
}
