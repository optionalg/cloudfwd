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
 * Names of properties file keys, as well as their default values and minimum
 * allowed values where applicable.
 *
 * @author ghendrey
 */
public class PropertyKeys {

  /* **************************** KEYS ************************* */
  /**
   * The authentication token for the Http Event Collector input on the Splunk
   * destination.
   */
  public static final String TOKEN = "splunk_hec_token";

  /**
   * The url and port number for the Splunk HEC endpoint or load balancer in
   * front of Splunk cluster. This can also be a comma-separated list of urls.
   * Example: https://127.0.0.1:8088
   */
  public static final String COLLECTOR_URI = "splunk_hec_url";

  /**
   * Host value for the data feed
   * If none, host field is set to hostname
   */
  public static final String HOST = "splunk_hec_host";

  /**
   * The Splunk index in which the data feed is stored
   * New Index must be created/exist on Splunk to store events.
   * Default: main
   */
  public static final String INDEX = "splunk_hec_index";

  /**
   * The source of the data feed
   * Default: based on sources EG: filename, network protocol
   */
  public static final String SOURCE = "splunk_hec_source";

  /**
   * The source type of events of data feed
   * Default: based on pre-defined source types EG:httpevent
   */
  public static final String SOURCETYPE = "splunk_hec_sourcetype";

  /**
   * Specifies whether to send to Splunk HEC /raw or /event endpoint.
   * Can be one of two values: "raw" or "event".
   * @see DEFAULT_HEC_ENDPOINT_TYPE
   */
  public static final String HEC_ENDPOINT_TYPE = "hec_endpoint_type";

  /**
   * Checkpoints are disabled by default. Checkpoints are a feature that can be
   * used ONLY when the sequence of Events sent through the connection has
   * monotonically ascending Comparable IDs. When checkpoints are enabled,
   * ConnectionCallbacks.checkpoint is called each time the highwater mark moves
   * forward in the monotonic sequence. The checkpoint moves forward when all
   * events have been delivered whose IDs are lower or equal to the highwater
   * mark provided in the ConnectionCallbacks.checkpoint call. By storing the
   * highwater mark to durable storage, the user of the Connection can recover
   * from a crash/cold start by reading the highwater mark out of storage and
   * resuming sending events based on the highwater mark. It is important to
   * understand that, because acknowledgments occur out of order, that while
   * a ConnectionCallbacks.acknowledged will be called for every event that is
   * acknowledged by splunk HEC, that ConnectionCallbacks.checkpoint need
   * not be called for each, and may skip over many acknowledged IDs landing
   * only on the highwater mark itself.
   */
  public static final String ENABLE_CHECKPOINTS = "enable_checkpoints";

  /**
   * If true, disables certificate validation and allows sending to non-HTTPs
   * endpoints. Defaults to false.
   */
  public static final String DISABLE_CERT_VALIDATION = "disable_certificate_validation";

  /**
   * Integer number of channels per internet socket address destination. Event
   * batches are load balanced across all channels. Note: A single URL may
   * resolve to multiple destinations based on DNS lookup.
   *
   * @see MOCK_FORCE_URL_MAP_TO_ONE
   * @see DEFAULT_CHANNELS_PER_DESTINATION
   */
  public static final String CHANNELS_PER_DESTINATION = "channels_per_dest";

  /**
   * If true, uses a mock HEC instead of a live Splunk instance. Useful for
   * running unit tests without a live Splunk instance.
   *
   * @see MOCK_HTTP_CLASSNAME
   */
  public static final String MOCK_HTTP_KEY = "mock_http";

  /**
   * If true, forces each url in COLLECTOR_URI to map to a single internet
   * socket address during DNS resolution. Defaults to false.
   */
  public static final String MOCK_FORCE_URL_MAP_TO_ONE = "mock_force_url_map_to_one";

  /**
   * If a channel is unresponsive (no acks received) for this many ms, the
   * channel is declared dead. A replacement channel is created, and all unacked
   * messages from the dead channel are re-sent on the replacement channel.
   *
   * @see RETRIES
   * @see DEFAULT_UNRESPONSIVE_MS
   */
  public static final String UNRESPONSIVE_MS = "unresponsive_channel_decom_ms";

  /**
   * Integer that caps the total number of channels across all destinations.
   *
   * @see CHANNELS_PER_DESTINATION
   * @see DEFAULT_MAX_TOTAL_CHANNELS
   */
  public static final String MAX_TOTAL_CHANNELS = "max_total_channels";

  /**
   * Integer that caps the total number of unacknowledged event batches per
   * channel. If the cap is reached, the channel will be considered full and no
   * more events will be sent through it until it receives acknowledgements from
   * Splunk software.
   *
   * @see DEFAULT_MAX_UNACKED_EVENT_BATCHES_PER_CHANNEL
   * @see MIN_UNACKED_EVENT_BATCHES_PER_CHANNEL
   */
  public static final String MAX_UNACKED_EVENT_BATCHES_PER_CHANNEL = "max_unacked_per_channel";

  /**
   * Character length of the internal buffer that fills up as events are passed
   * into the client. When full, the buffer is flushed and events are sent to
   * Splunk HEC.
   *
   * @see DEFAULT_EVENT_BATCH_SIZE
   * @see MIN_EVENT_BATCH_SIZE
   */
  public static final String EVENT_BATCH_SIZE = "event_batch_size";

  /**
   * Interval (in milliseconds) to poll Splunk HEC for event batch
   * acknowledgements.
   *
   * @see MIN_ACK_POLL_MS
   * @see DEFAULT_ACK_POLL_MS
   */
  public static final String ACK_POLL_MS = "ack_poll_ms";

  /**
   * Interval (in milliseconds) to poll Splunk HEC for health check in case a
   * Splunk indexer queue (and therefore channel) is full.
   *
   * @see MIN_HEALTH_POLL_MS
   * @see DEFAULT_HEALTH_POLL_MS
   */
  public static final String HEALTH_POLL_MS = "health_poll_ms";

  /**
   * Interval (in milliseconds) on which channels are drained and subsequently
   * destroyed and replaced.
   *
   * @see MIN_DECOM_MS
   * @see DEFAULT_DECOM_MS
   */
  public static final String CHANNEL_DECOM_MS = "channel_decom_ms";

  /**
   * Duration (in milliseconds) to wait for acknowledgements from Splunk
   * software after sending an event batch before reporting a send failure.
   *
   * @see DEFAULT_ACK_TIMEOUT_MS
   * @see MIN_ACK_TIMEOUT_MS
   */
  public static final String ACK_TIMEOUT_MS = "ack_timeout_ms";

  /**
   * Duration (in milliseconds) that Connection.send() blocks in case that all
   * channels are unavailable. If no channels become available before this
   * timeout, an HecConnectionTimeoutException is thrown.
   *
   * @see DEFAULT_BLOCKING_TIMEOUT_MS
   */
  public static final String BLOCKING_TIMEOUT_MS = "blocking_timeout_ms";

  /**
   * Name of the mock HTTP class used for running unit tests.
   *
   * @see MOCK_HTTP_KEY
   */
  public static final String MOCK_HTTP_CLASSNAME = "mock_http_classname";

  /**
   * Custom SSL Certificate Authority public cert content in X509 format.
   */
  public static final String SSL_CERT_CONTENT = "ssl_cert_content";

  /**
   * Custom SSL Certificate Authority public cert content in X509 format for
   * Splunk Cloud.
   */
  public static final String CLOUD_SSL_CERT_CONTENT = "cloud_ssl_cert_content";

  /**
   * If true, enables debug output at the HTTP level.
   */
  public static final String ENABLE_HTTP_DEBUG = "enable_http_debug";

  /**
   * The maximum number of attempts to resend EventBatch when a channel is
   * suspected of being unresponsive. Note: Retries always occur on newly
   * created channels that replace the dead channel.
   *
   * @see UNRESPONSIVE_MS
   * @see DEFAULT_RETRIES
   */
  public static final String RETRIES = "max_retries";
  
  /**
   * The maximum number of attempts to try preflight checks
   * @see DEFAULT_RETRIES
   */
  public static final String PREFLIGHT_RETRIES = "max_preflight_tries";
  
  /**
   * Channel Quiesce Timeout in MS define how much time to wait for a channel to 
   * drain all events. Channel will be killed if not quiesced in the time.
   * @see DEFAULT_CHANNEL_QUIESCE_TIMEOUT_MS
   */
  public static final String CHANNEL_QUIESCE_TIMEOUT_MS = "channel_quiesce_timeout_ms";
  

  /* **************************** REQUIRED KEYS ************************* */

  // Connection object cannot be instantiated without these keys being provided, either in overrides or cloudfwd.properties
  public static final String[] REQUIRED_KEYS = {TOKEN, COLLECTOR_URI};


  /* **************************** DEFAULTS ************************* */
  /**
   * Default value for EVENT_BATCH_SIZE property.
   *
   * @see EVENT_BATCH_SIZE
   */
  public static final String DEFAULT_EVENT_BATCH_SIZE = "32768"; //32k characters

  /**
   * Default value for ACK_POLL_MS property.
   *
   * @see ACK_POLL_MS
   */
  public static final String DEFAULT_ACK_POLL_MS = "1000"; //1sec

  /**
   * Default value for HEALTH_POLL_MS property.
   *
   * @see HEALTH_POLL_MS
   */
  public static final String DEFAULT_HEALTH_POLL_MS = "1000";  //1 sec

  /**
   * Default value for CHANNEL_DECOM_MS property.
   *
   * @see CHANNEL_DECOM_MS
   */
  public static final String DEFAULT_DECOM_MS = "600000";   //10 min

  /**
   * Default value for ACK_TIMEOUT_MS property.
   *
   * @see ACK_TIMEOUT_MS
   */
  public static final String DEFAULT_ACK_TIMEOUT_MS = "300000"; //5 min

  /**
   * Default value for BLOCKING_TIMEOUT_MS property.
   *
   * @see BLOCKING_TIMEOUT_MS
   */
  public static final String DEFAULT_BLOCKING_TIMEOUT_MS = "60000"; //1 min   

  /**
   * Default value for UNRESPONSIVE_MS property.
   *
   * @see UNRESPONSIVE_MS
   */
  public static final String DEFAULT_UNRESPONSIVE_MS = "-1"; //disabled by default

  /**
   * Default value for the RETRIES property.
   *
   * @see RETRIES
   */
  public static final String DEFAULT_RETRIES = "10";
  
 /**
   * Default value for the PREFLIGHT_RETRIES property.
   *
   * @see RETRIES
   */
  public static final String DEFAULT_PREFLIGHT_RETRIES = "3";  

  /**
   * Default value for the MAX_UNACKED_EVENT_BATCHES_PER_CHANNEL property.
   *
   * @see MAX_UNACKED_EVENT_BATCHES_PER_CHANNEL
   */
  public static final String DEFAULT_MAX_UNACKED_EVENT_BATCHES_PER_CHANNEL = "2";

  /**
   * Default value for the MAX_TOTAL_CHANNELS property. This is interpreted as
   * "unlimited" channels.
   *
   * @see MAX_TOTAL_CHANNELS
   */
  public static final String DEFAULT_MAX_TOTAL_CHANNELS = "-1";

  /**
   * Default value for the CHANNELS_PER_DESTINATION property.
   *
   * @see CHANNELS_PER_DESTINATION
   */
  public static final String DEFAULT_CHANNELS_PER_DESTINATION = "8";

  /**
   * Default value for the HEC_ENDPOINT_TYPE property.
   * @see HEC_ENDPOINT_TYPE
   */
  public static final String DEFAULT_HEC_ENDPOINT_TYPE = "raw";

  /**
   * By default checkpoints are disabled. When checkpoints are disabled the
   * ConnectionCallbacks.checkpoint method is still invoked, but its meaning is
   * identical to ConnectionCallbacks.acknowledged. It will be called once for
   * each acknowledged ID, and can occur out of order with respect to event IDs.
   * When disabled, the application does NOT have to send events in order of
   * monotonically increasing IDs.
   */
  public static final String DEFAULT_ENABLE_CHECKPOINTS = "false";
  
  /**
   * Channel Quiesce Timeout define how much time to wait for a channel to 
   * drain all events. Channel will be killed if not quiesced in the time.
   * @see CHANNEL_QUIESCE_TIMEOUT_MS
   */
  public static final String DEFAULT_CHANNEL_QUIESCE_TIMEOUT_MS = "180000";



  /* **************************** LIMITS ************************* */
  /**
   * Minimum allowed value for EVENT_BATCH_SIZE property.
   *
   * @see EVENT_BATCH_SIZE
   */
  public static final int MIN_EVENT_BATCH_SIZE = 0;

  /**
   * Minimum allowed value for ACK_POLL_MS property.
   *
   * @see ACK_POLL_MS
   */
  public static final long MIN_ACK_POLL_MS = 250;

  /**
   * Minimum allowed value for HEALTH_POLL_MS property.
   *
   * @see HEALTH_POLL_MS
   */
  public static final long MIN_HEALTH_POLL_MS = 1000;

  /**
   * Minimum allowed value for CHANNEL_DECOM_MS property.
   *
   * @see CHANNEL_DECOM_MS
   */
  public static final long MIN_DECOM_MS = 60000;

  /**
   * Minimum allowed value for ACK_TIMEOUT_MS property.
   *
   * @see ACK_TIMEOUT_MS
   */
  public static final long MIN_ACK_TIMEOUT_MS = 60000;  //60 sec

  /**
   * Minimum value for MAX_UNACKED_EVENT_BATCHES_PER_CHANNEL property. At 1,
   * every event batch post must wait for the ack to come back before another
   * event batch can be posted. Therefore, at 1, there is no pipelining of event
   * posts, and efficiency will be low.
   *
   * @see MAX_UNACKED_EVENT_BATCHES_PER_CHANNEL
   */
  public static final int MIN_UNACKED_EVENT_BATCHES_PER_CHANNEL = 1;
  
  /**
   * Minimum allowed value for CHANNEL_QUIESCE_TIMEOUT_MS property. Should be 
   * bigger than MIN_ACK_TIMEOUT_MS to let ack to be timed out.
   *
   * @see ACK_TIMEOUT_MS
   */
  public static final long MIN_CHANNEL_QUIESCE_TIMEOUT_MS = MIN_ACK_TIMEOUT_MS + 30000; 
  
}
