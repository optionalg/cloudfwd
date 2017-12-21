package com.splunk.cloudfwd.test.mock;

import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.Connections;
import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.test.util.AbstractConnectionTest;
import org.junit.Assert;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

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
    
}
