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
import com.splunk.cloudfwd.impl.http.HecIOManager;
import com.splunk.cloudfwd.impl.http.HttpPostable;
import java.util.List;
import org.apache.http.HttpEntity;

/**
 *
 * @author ghendrey
 */
public interface EventBatch extends HttpPostable {

  EventBatch add(Event event);

  /**
   * @return the ackId
   */
  Long getAckId();

  @Override
  HttpEntity getEntity();

  /**
   * @return the id
   */
  Comparable getId();

  int getLength();

  int getNumEvents();

  /**
   * @return the numTries
   */
  int getNumTries();

  /**
   * @return the acknowledged
   */
  boolean isAcknowledged();

  /**
   * @return the flushed
   */
  boolean isFlushed();

  boolean isTimedOut(long timeout);

  void post(HecIOManager ioManager);

  void prepareToResend();

  /**
   * @param ackId the ackId to set
   */
  void setAckId(Long ackId);

  /**
   * @param acknowledged the acknowledged to set
   */
  void setAcknowledged(boolean acknowledged);

    /**
     * Returs the type of the HEC endpoint that this EventBatch would be send to
     * @return the hec endpoint type
     */
    ConnectionImpl.HecEndpoint getTarget();

  String toString();
  
    /**
     * returns true if number of bytes in this EventBatch exceeds len
     * @param len
     * @return
     */
    public boolean isFlushable(int len);

    /**
     * @return the sendExceptions
     */
    List<Exception> getExceptions();
    
  LifecycleMetrics getLifecycleMetrics();
      
}
