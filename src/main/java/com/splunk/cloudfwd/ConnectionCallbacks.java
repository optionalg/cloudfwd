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

import com.splunk.cloudfwd.impl.EventBatchImpl;

/**
 * Callers of Connection's send methods must provide an implementation of
 * ConnectionCallbacks to receive asynchronous notifications about their
 * in-flight events.
 *
 * @author ghendrey
 */
public interface ConnectionCallbacks {

  /**
   * The **acknowledged** function is called once for each EventBatchImpl that has been
 replicated amongst Splunk indexers. Events sent via Connection.send are
 internally batched, even if there is only one event in an EventBatchImpl. This is
 why acknowledgments are per-EventBatchImpl and not per-Event.
   *
   * @param events
   */
  public void acknowledged(EventBatch events);

  /**
   * The **failed** function is called if there is a failure to deliver EventBatch to your
 Splunk platform instance.
   *
   * @param events
   * @param ex
   */
  public void failed(EventBatch  events, Exception ex);

  /**
   * The **checkpoint** function is called when there are no unacknowledged events in-flight with an id less than or equal to
   * events.getId(). If checkpoints are disabled, the checkpoint callback behaves identically to the acknowledged callback.
   * @param events
   */
  public void checkpoint(EventBatch events);
  
    /**
     * Invoked when a system problem occurs.
     * @param e
     */
    public void systemError(Exception e);
    
    /**
     * Invoked when a system condition occurs that could be indicative of a problem. For example, server responses such 
     * as 503 or indexer is busy are not errors, but the application may want to record their occurrence.
     * @param e
     */
    public void systemWarning(Exception e);

}
