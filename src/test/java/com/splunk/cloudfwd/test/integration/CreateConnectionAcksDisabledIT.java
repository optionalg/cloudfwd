package com.splunk.cloudfwd.test.integration;

import com.splunk.cloudfwd.LifecycleEvent;
import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.error.HecNoValidChannelsException;
import com.splunk.cloudfwd.error.HecServerErrorResponseException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

import static com.splunk.cloudfwd.PropertyKeys.BLOCKING_TIMEOUT_MS;

/**
 * Created by eprokop on 10/4/17.
 */
public class CreateConnectionAcksDisabledIT extends AbstractReconciliationTest {
    @Override
    protected int getNumEventsToSend() {
        return 1;
    }

    @Test
    public void createConnectionWithAcksDisabled() throws InterruptedException {
        super.sendEvents(false, false);
        assertAllChannelsFailed(HecServerErrorResponseException.class,
                "HecServerErrorResponseException{serverRespObject=HecErrorResponseValueObject{text=ACK is disabled, code=14, invalidEventNumber=-1}, " +
                        "httpBodyAndStatus=HttpBodyAndStatus{statusCode=400, body={\"text\":\"ACK is disabled\",\"code\":14}}, " +
                        "lifecycleType=ACK_DISABLED, url=https://127.0.0.1:8088, errorType=RECOVERABLE_CONFIG_ERROR, context=null}");
        connection.close();
    }

    @Override
    protected Properties getProps() {
        Properties p = super.getProps();
        p.setProperty(PropertyKeys.TOKEN, createTestToken(null, false));
        p.setProperty(PropertyKeys.MAX_TOTAL_CHANNELS, "1");
        p.put(BLOCKING_TIMEOUT_MS, "3000");
        p.put(PropertyKeys.EVENT_BATCH_SIZE, "0");
        return p;
    }

    @Override
    protected boolean shouldSendThrowException() { return true; }

    @Override
    protected boolean isExpectedSendException(Exception e) { return e instanceof HecNoValidChannelsException; }
}
        
