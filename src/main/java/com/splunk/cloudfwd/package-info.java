/**
 * The Connection class is used to send Events to Splunk HEC with indexer acknowledgements. The Connection 
 * supports buffering. When buffering is used, Events sent through the Connection are buffered and sent as
 * an EventBatch.
 *
 * @since 1.0
 *
 */
package com.splunk.cloudfwd;
