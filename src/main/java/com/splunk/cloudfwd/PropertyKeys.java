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
 * Names of properties file keys, as well as their default values and minimum allowed values where applicable.
 * @author ghendrey
 */
public class PropertyKeys {
  
  public static final String TOKEN = "token";
  public static final String COLLECTOR_URI = "url";
  public static final String HOST = "host";
  public static final String DISABLE_CERT_VALIDATION = "disableCertificateValidation";
  public static final String CHANNELS_PER_DESTINATION = "channels_per_dest";
  public static final String MOCK_HTTP_KEY = "mock_http";
  public static final String MOCK_FORCE_URL_MAP_TO_ONE = "mock_force_url_map_to_one";
  public static final String UNRESPONSIVE_MS = "unresponsive_channel_decom_ms";
  public static final String MAX_TOTAL_CHANNELS = "max_total_channels";
  public static final String MAX_UNACKED_EVENT_BATCHES_PER_CHANNEL = "max_unacked_per_channel";
  public static final String EVENT_BATCH_SIZE = "event_batch_size";
  public static final String ACK_POLL_MS = "ack_poll_ms";
  public static final String HEALTH_POLL_MS = "health_poll_ms";  
  public static final String CHANNEL_DECOM_MS = "channel_decom_ms";  
  public static final String ACK_TIMEOUT_MS = "ack_timeout_ms";  
  public static final String BLOCKING_TIMEOUT_MS = "blocking_timeout_ms";  
  public static final String MOCK_HTTP_CLASSNAME = "mock_http_classname";  
  public static final String SSL_CERT_CONTENT = "ssl_cert_content";
  public static final String CLOUD_SSL_CERT_CONTENT = "cloud_ssl_cert_content";
  public static final String ENABLE_HTTP_DEBUG = "enable_http_debug";  
  
  public static final String DEFAULT_EVENT_BATCH_SIZE = "32768"; //32k characters
  public static final String DEFAULT_ACK_POLL_MS = "1000"; //1sec
  public static final String DEFAULT_HEALTH_POLL_MS = "1000";  //1 sec 
  public static final String DEFAULT_DECOM_MS = "600000";   //10 min
  public static final String DEFAULT_ACK_TIMEOUT_MS = "300000"; //5 min  
  public static final String DEFAULT_BLOCKING_TIMEOUT_MS = "60000"; //1 min   
  
  public static final int MIN_EVENT_BATCH_SIZE = 0;
  public static final long MIN_ACK_POLL_MS = 250;
  public static final long MIN_HEALTH_POLL_MS = 1000; 
  public static final long MIN_DECOM_MS = 60000;
  public static final long MIN_ACK_TIMEOUT_MS = 60000;  //60 sec



}