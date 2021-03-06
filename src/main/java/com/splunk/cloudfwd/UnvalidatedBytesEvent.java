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
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.io.IOUtils;

/**
 *
 * @author ghendrey
 */
public class UnvalidatedBytesEvent implements Event{
  // No access to Connection instance so must use SLF4J logger
  private static final Logger LOG = LoggerFactory.getLogger(UnvalidatedBytesEvent.class.getName());

  private byte[] bytes;
  private Comparable id;

  public UnvalidatedBytesEvent(byte[] bytes, Comparable id) {
    if(bytes.length < 1){
      throw new IllegalArgumentException("Byte Array Empty");
    }
    this.bytes = bytes;
    this.id = id;
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
  public void writeTo(OutputStream out) throws IOException {
    out.write(bytes);
  }

  @Override
  public ConnectionImpl.HecEndpoint getTarget() {
    throw new UnsupportedOperationException("UnvalidatedByteEvent does not implement getTarget"); 
  }

  @Override
  public Type getType() {
    return Event.Type.UNKNOWN;
  }

  @Override
  public InputStream getInputStream() {
    return new ByteArrayInputStream(bytes);
  }
  
  @Override 
  public String toString(){
    try {
      return IOUtils.toString(getInputStream(), "UTF-8");
    } catch (IOException ex) {
      LOG.error(ex.getMessage(), ex);
      throw new RuntimeException(ex.getMessage(), ex);
    }
  }  

  @Override
  public int length() {
    return bytes.length;
  }
  
}
