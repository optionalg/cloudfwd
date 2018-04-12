package com.splunk.cloudfwd.test.integration;

import com.splunk.cloudfwd.ConnectionSettings;
import com.splunk.cloudfwd.LifecycleEvent;
import com.splunk.cloudfwd.error.HecMaxRetriesException;
import com.splunk.cloudfwd.error.HecNoValidChannelsException;
import com.splunk.cloudfwd.error.HecServerErrorResponseException;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by eprokop on 10/4/17.
 */
public class SendOnConnectionAcksDisabledIT extends AbstractReconciliationTest {
    @Override
    protected int getNumEventsToSend() {
        return 0;
    }

    @Test
    public void sendOnConnectionWithAcksDisabled() throws InterruptedException {
        super.sendEvents(false,false);
        assertAllChannelsFailed(HecServerErrorResponseException.class, 
                "HecServerErrorResponseException{serverRespObject=HecErrorResponseValueObject{text=ACK is disabled, code=14, invalidEventNumber=-1}, " +
                        "httpBodyAndStatus=HttpBodyAndStatus{statusCode=400, body={\"text\":\"ACK is disabled\",\"code\":14}}, " +
                        "lifecycleType=ACK_DISABLED, url=https://127.0.0.1:8088, errorType=RECOVERABLE_CONFIG_ERROR, context=null}");
        connection.close();
    }

    @Override
    protected void configureProps(ConnectionSettings settings) {
        super.configureProps(settings);
        settings.setToken(createTestToken(null, false));
        settings.setMaxTotalChannels(1);
    }
    
    @Override
    protected boolean shouldSendThrowException() { return true; }
    
    @Override
    protected boolean isExpectedSendException(Exception e) { return e instanceof HecNoValidChannelsException; }
}
