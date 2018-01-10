package com.splunk.cloudfwd.test.mock;

import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.Connections;
import com.splunk.cloudfwd.error.HecConnectionStateException;
import com.splunk.cloudfwd.error.HecServerErrorResponseException;
import com.splunk.cloudfwd.test.util.BasicCallbacks;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.splunk.cloudfwd.PropertyKeys.*;

/**
 * Created by mhora on 9/11/17.
 */
public class PropertiesConfigurationTest {
    private static final Logger LOG = LoggerFactory.getLogger(PropertiesConfigurationTest.class.getName());

    private static final BasicCallbacks callbacks = new BasicCallbacks(0);
//    private Connection connection;
    
 // this whole test has to be rethought. It is opening connections to bogus hostname, as part of instantiating the connection
   // which hangs for very long time before timeout.
    
    // PropertiesHelper Configuration Tests
    @Test
    public void testOverrideProperties() throws MalformedURLException {
        // Need connection object to pass into ConnectionSettings constructor for failed() callback
        String testToken = "foo-token";
        String testEventBatchSize = "100";
        
        Properties overrides = new Properties();
        overrides.put(TOKEN, testToken);
        overrides.put(COLLECTOR_URI, "https://inputs1.kinesis1.foo.com:8088");
        overrides.put(EVENT_BATCH_SIZE, testEventBatchSize);
        
        overrides.put(MOCK_HTTP_KEY, "true");
        overrides.put(MAX_TOTAL_CHANNELS, "1");

        Connection connection = Connections.create(callbacks, overrides);

        // Overrides took effect
        Assert.assertEquals("https://inputs1.kinesis1.foo.com:8088", connection.getSettings().getUrlString()); 
        Assert.assertEquals(testToken, connection.getSettings().getToken());
        // Override took effect and value stored as Int
        Assert.assertEquals(Long.parseLong(testEventBatchSize), connection.getSettings().getEventBatchSize());
        // Set to default value 
        Assert.assertEquals(DEFAULT_ACK_POLL_MS, connection.getSettings().getAckPollMS());
    }

    // should fail without sufficient configuration
    @Test(expected = HecConnectionStateException.class)
    public void testProperties() throws MalformedURLException {
        // Need connection object to pass into ConnectionSettings constructor for failed() callback
        Properties overrides = new Properties();
        Connection connection = Connections.create(callbacks, overrides);
    }
}
