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

import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import com.splunk.cloudfwd.http.HecIOManager;
import com.splunk.cloudfwd.http.HttpPostable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.entity.AbstractHttpEntity;

/**
 *
 * @author ghendrey
 */
public class ZeroCopyEventBatch implements HttpPostable{

  private final ByteBuffer buf;

  public ZeroCopyEventBatch(ByteBuffer buf) {
    this.buf = buf;
  }
  
  public InputStream getInputStream(){
    buf.rewind();
    return new ByteBufferBackedInputStream(buf);
  }

  public long getContentLength() {
    return buf.limit();
  }

  @Override
  public void post(HecIOManager ioManager) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public boolean isFlushed() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public HttpEntity getEntity() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  private class ByteBufferHttpEntity extends AbstractHttpEntity{
  ZeroCopyEventBatch eventBatch;
  
  public ByteBufferHttpEntity(ZeroCopyEventBatch b){
    this.eventBatch = b;
  }

  @Override
  public boolean isRepeatable() {
    return true;
  }

  @Override
  public long getContentLength() {
    return eventBatch.getContentLength();
  }

  @Override
  public InputStream getContent() throws IOException, UnsupportedOperationException {
    return eventBatch.getInputStream();
  }

  @Override
  public void writeTo(OutputStream outstream) throws IOException {
    IOUtils.copy(getContent(), outstream);
  }

  @Override
  public boolean isStreaming() {
    return false;
  }
  
}

}
