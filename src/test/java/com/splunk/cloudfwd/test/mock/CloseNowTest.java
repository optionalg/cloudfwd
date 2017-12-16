package com.splunk.cloudfwd.test.mock;

import com.splunk.cloudfwd.ConnectionSettings;
import com.splunk.cloudfwd.error.HecConnectionStateException;
import com.splunk.cloudfwd.test.util.AbstractConnectionTest;
import com.splunk.cloudfwd.test.util.BasicCallbacks;
import org.junit.Test;

/**
 * Created by eprokop on 11/8/17.
 */
public class CloseNowTest extends AbstractConnectionTest {
    @Test
    public void sendAndCloseNow() throws InterruptedException {
        super.sendEvents(true, true);
    }
    
    @Override
    protected void configureProps(ConnectionSettings settings) {
        settings.setMockHttp(true);
    }

    @Override
    protected int getNumEventsToSend() {
        return 500000;
    }
    
    @Override
    protected BasicCallbacks getCallbacks() {
        return new BasicCallbacks(getNumEventsToSend()) {
            @Override
            public boolean shouldFail() {
                return true;
            }

            @Override
            public boolean isExpectedFailureType(Exception e) {
                if (e instanceof HecConnectionStateException && 
                        ((HecConnectionStateException)e).getType() == HecConnectionStateException.Type.CONNECTION_CLOSED) {
                    return true;
                } else if (e instanceof HecConnectionStateException &&
                        ((HecConnectionStateException)e).getType() == HecConnectionStateException.Type.NO_HEC_CHANNELS) {
                    return true;
                }
                return false;
            }
        };
    }
    
}
