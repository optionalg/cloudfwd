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

import com.splunk.cloudfwd.impl.ConnectionImpl;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * An Event can be a JSON document or a blob of text. Every Event must have a comparable id. For each
 * Event sent to the Connection, the id must be greater than the previously sent id. That is, ids must
 * be monotonically ascending. Ids can be integers, strings, or any other comparable.
 * @author ghendrey
 */
public interface Event {
  public enum Type{TEXT, JSON, UNKNOWN}
  public Comparable getId();
  public byte[] getBytes();
  public InputStream getInputStream();
  public void writeTo(OutputStream out) throws IOException;
  public ConnectionImpl.HecEndpoint getTarget();
  public Type getType();
  public int length();
}
