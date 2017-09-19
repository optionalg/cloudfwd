package com.splunk.cloudfwd.impl.util;

import org.slf4j.Logger;

/**
 * Created by mhora on 9/18/17.
 */
public interface HecLoggerFactory {
    Logger getLogger(String name);
}
