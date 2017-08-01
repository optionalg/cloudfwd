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
package com.splunk.cloudfwd.http;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.util.EntityUtils;

/**
 *
 * @author ghendrey
 */
public abstract class AbstractHttpCallback implements FutureCallback<HttpResponse> {

  private static final Logger LOG = Logger.getLogger(AbstractHttpCallback.class.
          getName());

  @Override
  final public void completed(HttpResponse response) {
    int code = response.getStatusLine().getStatusCode();
    try {
      String reply = EntityUtils.toString(response.getEntity(), "utf-8");
      completed(reply, code);
    } catch (IOException e) {      
      LOG.log(Level.SEVERE, "failed to unmarshal response", e);
    }      
  }

  public abstract void completed(String reply, int code);

}
