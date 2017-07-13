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

import com.splunk.logging.HttpEventCollectorSender;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author ghendrey
 */
public class SenderFactory {

  Properties props = new Properties();

  public SenderFactory() {
    try {
      InputStream is = getClass().getResourceAsStream("/lb.properties");
      if (null == is) {
        throw new RuntimeException("can't find /lb.properties");
      }
      props.load(is);
    } catch (IOException ex) {
      Logger.getLogger(SenderFactory.class.getName()).
              log(Level.SEVERE, null, ex);
      throw new RuntimeException(ex.getMessage(), ex);
    }
  }

  public HttpEventCollectorSender createSender() {
    String url;
    String token;
    long batchInterval;
    long batchSize;
    long batchCount;
    boolean ack;
    String ackUrl;
    boolean disableCertificateValidation;
    try {
      url = props.getProperty("url");
      token = props.getProperty("token");
      batchInterval = Long.parseLong(props.getProperty("batch_interval"));
      batchSize = Long.parseLong(props.getProperty("batch_size_bytes"));
      batchCount = Long.parseLong(props.getProperty("batch_size_count"));
      ack = Boolean.parseBoolean(props.getProperty("ack"));
      ackUrl = props.getProperty("ackUrl");
      disableCertificateValidation = Boolean.parseBoolean(props.getProperty("disableCertificateValidation"));
    } catch (Exception e) {
      throw new RuntimeException("problem parsing lb.properties", e);
    }

    HttpEventCollectorSender sender = new HttpEventCollectorSender(
            url,
            token,
            batchInterval,
            batchCount,
            batchSize,
            "parallel", 
            ack,
            ackUrl, new HashMap());
    if(disableCertificateValidation){
      sender.disableCertificateValidation();
    }
    return sender;
  }

  String getLogger() {
    return "FOO-FIXME";
  }

  String getSeverity() {
    return "INFO";
  }

}
