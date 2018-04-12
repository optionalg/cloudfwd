package com.splunk.cloudfwd.test.integration;

import com.splunk.cloudfwd.ConnectionSettings;
import com.splunk.cloudfwd.Connections;
import com.splunk.cloudfwd.LifecycleEvent;
import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.error.HecNoValidChannelsException;
import com.splunk.cloudfwd.error.HecServerErrorResponseException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

import static com.splunk.cloudfwd.PropertyKeys.BLOCKING_TIMEOUT_MS;

/**
 * Created by eprokop on 10/5/17.
 */
public class SendOnConnectionInvalidTokenIT extends AbstractReconciliationTest {
    
    // Scenario: Connection created with invalid token
    // Expected behavior: Connection succeeds to instantiate and send throws proper exception
    @Test
    public void sendOnConnectionWithInvalidToken() throws InterruptedException {
        super.sendEvents(false, false);
        assertAllChannelsFailed(HecServerErrorResponseException.class,
                "HecServerErrorResponseException{serverRespObject=HecErrorResponseValueObject{text=Invalid token, code=4, invalidEventNumber=-1}, " +
                        "httpBodyAndStatus=HttpBodyAndStatus{statusCode=403, body={\"text\":\"Invalid token\",\"code\":4}}, " +
                        "lifecycleType=INVALID_TOKEN, url=https://127.0.0.1:8088, errorType=RECOVERABLE_CONFIG_ERROR, context=null}");
        connection.close(); 
    }
    
    @Override
    protected int getNumEventsToSend() {
        return 1;
    }
    
    @Override
    protected void configureProps(ConnectionSettings settings) {
        super.configureProps(settings);
        settings.setToken("invalid_token");
        settings.setMaxTotalChannels(1);
        settings.setBlockingTimeoutMS(3000);
        settings.setEventBatchSize(0);
    }
    
    @Override
    protected boolean shouldSendThrowException() { return true; }
    
    @Override
    protected boolean isExpectedSendException(Exception e) { return e instanceof HecNoValidChannelsException; }
}
