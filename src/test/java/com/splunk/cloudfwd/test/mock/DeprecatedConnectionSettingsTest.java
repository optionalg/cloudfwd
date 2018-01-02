package com.splunk.cloudfwd.test.mock;

import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.Connections;
import com.splunk.cloudfwd.PropertyKeys;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static com.splunk.cloudfwd.PropertyKeys.DEFAULT_BLOCKING_TIMEOUT_MS;

/**
 * Created by mhora on 12/20/17.
 */
public class DeprecatedConnectionSettingsTest extends BatchedVolumeTest {
    protected int numToSend = 1000000;

    protected Connection createAndConfigureConnection(){
        Properties props = new Properties();
        props.putAll(getDeprecatedTestProps());
        props.put(PropertyKeys.ACK_TIMEOUT_MS, "1000000"); //we don't want the ack timout kicking in
        props.put(PropertyKeys.UNRESPONSIVE_MS, "-1"); //no dead channel detection
        props.put(PropertyKeys.TOKEN, "ohhai");
        props.put(PropertyKeys.CHANNELS_PER_DESTINATION, "3");
        props.put(PropertyKeys.MAX_TOTAL_CHANNELS, "-1");
        boolean didThrow = false;
        Connection connection = null;
        try {
            connection = Connections.create(callbacks, props);
        } catch (Exception e){
            e.printStackTrace();
            didThrow = true;
            if(!connectionInstantiationShouldFail()){
                e.printStackTrace();
                Assert.fail("Connection instantiation should not have failed, but it did: " +e);
            }else{
                if(! isExpectedConnInstantiationException(e)){
                    Assert.fail("Connection instantiation failure was expected, but we didn't get the *expected* Exception.  Got: " + e);
                }
            }
        }
        if(!didThrow && connectionInstantiationShouldFail()){
            Assert.fail("expected a Connection instantiation Exception to be caught. None was caught.");
        }
        
        if(null ==connection){
            return null;
        }
        configureConnection(connection);
        return connection;
    }

    protected Properties getDeprecatedTestProps() {
        Properties props = new Properties();
        try (InputStream is = getClass().getResourceAsStream(
                getTestPropertiesFileName())) {
            if (null != is) {
                props.load(is);
            } else {
                LOG.trace("No test_defaults.properties found on classpath");
            }
        } catch (IOException ex) {
            LOG.error(ex.getMessage(), ex);
        }
        if (Boolean.parseBoolean(props.getProperty("enabled", "false"))) {
            return props;
        } else {
            LOG.warn("test.properties disabled, using cloudfwd.properties only");
            return new Properties(); //ignore test.properties
        }
    }
    
    @Test
    public void checkPropertiesCorrectlySet() {
        // Check that override provided in Properties object was correctly parsed to JsonNode and then into ConnectionSettings property
        Assert.assertEquals("ohhai", connection.getSettings().getToken());
        // Check that correct numeric type is set by ConnectionSettings setter
        Assert.assertEquals(3, connection.getSettings().getChannelsPerDestination());
        // Check that value of -1 was correctly interpreted and set to max value
        Assert.assertEquals(Integer.MAX_VALUE, connection.getSettings().getMaxTotalChannels());
        // Set to default value 
        Assert.assertEquals(DEFAULT_BLOCKING_TIMEOUT_MS, connection.getSettings().getBlockingTimeoutMS());


    }
}
