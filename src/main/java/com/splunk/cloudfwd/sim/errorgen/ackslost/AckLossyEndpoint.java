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
package com.splunk.cloudfwd.sim.errorgen.ackslost;

import com.splunk.cloudfwd.http.HecIOManager;
import com.splunk.cloudfwd.sim.AckEndpoint;
import com.splunk.cloudfwd.sim.AcknowledgementEndpoint;
import java.util.Collections;
import java.util.Map;
import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;

/**
 * basically turns off everything in the AckEndpoint except for generating the next ackId
 * @author ghendrey
 */
public class AckLossyEndpoint extends AckEndpoint implements AcknowledgementEndpoint {


  @Override
  public void close() {
    super.close();
  }

  @Override
  public long nextAckId() {
    return ackId.incrementAndGet();
  }

  @Override
  public void pollAcks(HecIOManager ackMgr, FutureCallback<HttpResponse> cb) {
    //when we poll for acks, we will always return no ackIDs. Models an indexer that takes forever to
    //index events
    Map resp = Collections.EMPTY_MAP;
    resp.put("acks", Collections.EMPTY_MAP);
    cb.completed(getResult(resp));
  }

  @Override
  public void start() {

  }

}
