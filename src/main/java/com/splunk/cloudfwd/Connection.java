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

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

/**
 *
 * @author ghendrey
 */
public class Connection implements Closeable{
  LoadBalancer lb;
  
  public Connection(){
    this.lb = new LoadBalancer();
  }
  
  public Connection(Properties settings){
    this.lb = new LoadBalancer(settings);
  }

  @Override
  public void close()  {
    lb.close();
  }
  
  public void send(String msg) throws TimeoutException {
    lb.send(msg);
  }
  
  public ConnectionState getConnectionState(){
    return lb.getConnectionState();
  }
  
}
