package com.splunk.cloudfwd;

import org.slf4j.Logger;

/**
 * @author mhora
 */
public interface HecLoggerFactory {
    Logger getLogger(String name);
}
