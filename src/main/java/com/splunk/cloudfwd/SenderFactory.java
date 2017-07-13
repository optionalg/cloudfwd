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

  public static final String EVENT_POST_URL_KEY = "url";
  public static final String ACK_POST_URL_KEY = "ackUrl";
  public static final String USE_ACKS_KEY = "ackl";
  public static final String TOKEN_KEY = "token";
  public static final String BATCH_COUNT_KEY = "batch_size_count";
  public static final String BATCH_BYTES_KEY = "batch_size_bytes";
  public static final String BATCH_INTERVAL_KEY = "batch_interval";
  public static final String DISABLE_CERT_VALIDATION_KEY = "disableCertificateValidation";
  public static final String SEND_MODE_KEY = "send_mode";

  private String url;
  private String token;
  private long batchInterval;
  private long batchSize;
  private long batchCount;
  private boolean ack;
  private String ackUrl;
  private boolean disableCertificateValidation;
  private String sendMode;
  
  
  
  Properties props = new Properties();

  public SenderFactory(Properties overrides) {
    this(); //setup all defaults by calling SenderFactory() empty constr
    this.props.putAll(overrides);
    validateAllSettings();
  }

  /**
   * create SenderFactory with default properties read from lb.properties file
   */
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

    try {
      url = props.getProperty(EVENT_POST_URL_KEY);
      token = props.getProperty(TOKEN_KEY);
      batchInterval = Long.parseLong(props.getProperty(BATCH_INTERVAL_KEY, "0"));
      batchSize = Long.parseLong(props.getProperty(BATCH_BYTES_KEY, "100"));
      batchCount = Long.parseLong(props.getProperty(BATCH_COUNT_KEY, "65536")); //64k
      ack = Boolean.parseBoolean(props.getProperty(USE_ACKS_KEY, "true")); //default is use acks
      ackUrl = props.getProperty(ACK_POST_URL_KEY);
      disableCertificateValidation = Boolean.parseBoolean(props.getProperty(
              DISABLE_CERT_VALIDATION_KEY, "false"));
      sendMode = props.getProperty(SEND_MODE_KEY, "parallel");
      validateAllSettings();
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
    if (disableCertificateValidation) {
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

  private void validateAllSettings() {
    if (! (sendMode.equals("sequential") || sendMode.equals("parallel")))  {
        throw new IllegalArgumentException("Invalid setting for " + SEND_MODE_KEY+": " + sendMode);
    }
    //TODO FIXME validate all other properties
  }

}
