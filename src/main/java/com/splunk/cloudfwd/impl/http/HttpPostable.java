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

import org.apache.http.HttpEntity;

/**
 * This interface exists mainly to allow us EventBatch to provide an HttpEntity. This allows EventBatch to be responsible for
 * how it provides bytes to http POST. In particular, it allows EventBatchImpl to provide a customized HttpEventBatchEntity
 * that can stream data out of individual Events, avoiding copies.
 * @author ghendrey
 */
public interface HttpPostable {
  //public void setSender(HttpSender sender);
  public void post(HecIOManager ioManager);
  public boolean isFlushed();
  public HttpEntity getEntity();
  
}
