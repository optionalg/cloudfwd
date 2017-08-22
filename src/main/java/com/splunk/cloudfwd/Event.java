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
 * Event can be either a JSON document, or a blob of text. Every Event must have a Comparable id. For each
 * Event sent to the Connection, the id must be must be greater than the previously sent id. That is, ids must
 * be monotonically ascending. Ids can be integers, String, or any other Comparable.
 * @author ghendrey
 */
public interface Event {
  @Override
  public String toString();
  public boolean isJson();
  public Comparable getId();
}
