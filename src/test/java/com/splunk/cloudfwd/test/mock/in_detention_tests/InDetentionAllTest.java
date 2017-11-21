package com.splunk.cloudfwd.test.mock.in_detention_tests;

import com.splunk.cloudfwd.ConnectionCallbacks;
import com.splunk.cloudfwd.Connections;
import com.splunk.cloudfwd.LifecycleEvent;
import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import com.splunk.cloudfwd.error.HecNoValidChannelsException;
import com.splunk.cloudfwd.test.mock.AbstractExceptionOnSendTest;
import com.splunk.cloudfwd.test.mock.in_detention_tests.AbstractInDetentionTest;
import com.splunk.cloudfwd.test.util.BasicCallbacks;
import com.splunk.cloudfwd.error.HecServerErrorResponseException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

import static com.splunk.cloudfwd.PropertyKeys.BLOCKING_TIMEOUT_MS;
import static com.splunk.cloudfwd.PropertyKeys.MOCK_HTTP_CLASSNAME;

/**
 * Created by mhora on 10/4/17.
 */
public class InDetentionAllTest extends AbstractExceptionOnSendTest {
    @Override
    protected Properties getProps() {
        Properties props = new Properties();
                props.put(MOCK_HTTP_CLASSNAME,
                        "com.splunk.cloudfwd.impl.sim.errorgen.indexer.InDetentionEndpoints");
        props.put(BLOCKING_TIMEOUT_MS, "500");
        props.put(PropertyKeys.UNRESPONSIVE_MS, "-1"); //no dead channel detection
        props.put(PropertyKeys.MAX_TOTAL_CHANNELS, "2");

        return props;
    }

    @Test
    public void sendToIndexersInDetention() throws InterruptedException {
        super.sendEvents(false, false);
        assertAllChannelsFailed(HecServerErrorResponseException.class,
                "HecServerErrorResponseException{serverRespObject=HecErrorResponseValueObject{text=null, code=-1, invalidEventNumber=-1}, " +
                        "httpBodyAndStatus=HttpBodyAndStatus{statusCode=404, body=Not Found}, " +
                        "lifecycleType=INDEXER_IN_DETENTION, url=https://127.0.0.1:8088, errorType=RECOVERABLE_CONFIG_ERROR, context=null}");}
    
    @Override
    protected boolean isExpectedSendException(Exception ex) {
        if (ex instanceof HecConnectionTimeoutException) return true;
        return false;
    }
}
