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
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.io.IOUtils;

/**
 *
 * @author ghendrey
 */
public class UnvalidatedByteBufferEvent implements Event {
  // No access to Connection instance so must use SLF4J logger
  private static final Logger LOG = LoggerFactory.getLogger(UnvalidatedByteBufferEvent.class.getName());

  private final ByteBuffer buf;
  private final Comparable id;

  public UnvalidatedByteBufferEvent(ByteBuffer buf, Comparable id) {
    this.buf = buf;
    this.id = id;
  }

  public InputStream getInputStream() {
    buf.rewind();
    return new ByteBufferBackedInputStream(buf);
  }

  @Override
  public Comparable getId() {
    return this.id;
  }

  /**
   * Warning: This method will throw UnsupportedOpertationException if called. You should use getInputStream() or
   * write() instead
   *
   * @return
   */
  @Override
  public byte[] getBytes() {
        throw new UnsupportedOperationException(
            "UnvalidatedByteBufferEvent does not implement getBytes");
    /*
    buf.rewind();
    byte[] a = new byte[buf.remaining()];
    buf.get(a);
    return a;
    */
  }

  @Override
  public void writeTo(OutputStream out) throws IOException {    
    IOUtils.copy(getInputStream(), out);
  }

  @Override
  public ConnectionImpl.HecEndpoint getTarget() {
    throw new UnsupportedOperationException(
            "UnvalidatedByteBufferEvent does not implement getTarget");
  }

  @Override
  public Type getType() {
    return Event.Type.UNKNOWN;
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
    buf.rewind();
    return buf.remaining();
  }
 
}
