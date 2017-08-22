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
 * Callers of Connection's send methods must provide an implementation of
 * ConnectionCallbacks to receive asynchronous notifications about their
 * in-flight events.
 *
 * @author ghendrey
 */
public interface ConnectonCallbacks {

  /**
   * Will be called once for each EventBatch that is acknowledged (has been
   * replicated amongst Splunk Indexers). Events sent via Connection.send are
   * internally batched (even if their is only one Event in an EventBatch),
   * which is why acknowledgments are per-EventBatch and not per-Event.
   *
   * @param events
   */
  public void acknowledged(EventBatch events);

  /**
   * Failed will be called if their is a failure to deliver EventBatch to
   * Splunk.
   *
   * @param events
   * @param ex
   */
  public void failed(EventBatch events, Exception ex);

  /**
   * Checkpoint is called when there are no unacknowledged Events in-flight with an id less than or equal to
   * events.getId().
   * @param events
   */
  public void checkpoint(EventBatch events);

}
