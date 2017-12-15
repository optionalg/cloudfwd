package com.splunk.cloudfwd.test.integration;

import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.error.HecNoValidChannelsException;
import com.splunk.cloudfwd.error.HecServerErrorResponseException;
import org.junit.Test;

import java.util.Properties;

/**
 * Created by eprokop on 10/5/17.
 */
public class CreateConnectionInvalidTokenIT extends AbstractReconciliationTest {
    
    // Scenario: Connection created with invalid token
    // Expected behavior: Connection succeeds to instantiate and send throws proper exception
    @Test
    public void createConnectionWithInvalidToken() throws InterruptedException {
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
    protected Properties getProps() {
        Properties p = super.getProps();
        p.put(PropertyKeys.TOKEN, "invalid_token");
        p.setProperty(PropertyKeys.MAX_TOTAL_CHANNELS, "1");
        p.setProperty(PropertyKeys.BLOCKING_TIMEOUT_MS, "3000");
        p.setProperty(PropertyKeys.EVENT_BATCH_SIZE, "0");
        return p;
    }
    
    @Override
    protected boolean shouldSendThrowException() { return true; }
    
    @Override
    protected boolean isExpectedSendException(Exception e) { return e instanceof HecNoValidChannelsException; }
}
