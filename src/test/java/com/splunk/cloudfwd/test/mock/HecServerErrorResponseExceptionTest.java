package com.splunk.cloudfwd.test.mock;

import com.splunk.cloudfwd.error.HecServerErrorResponseException;
import static com.splunk.cloudfwd.LifecycleEvent.Type.EVENT_POST_OK;
import com.splunk.cloudfwd.impl.http.HecErrorResponseValueObject;
import com.splunk.cloudfwd.impl.http.HttpBodyAndStatus;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by mhora on 9/14/17.
 */
public class HecServerErrorResponseExceptionTest {

    @Test
    public void getError() {
        HttpBodyAndStatus b = new HttpBodyAndStatus(0, "foo");
        HecErrorResponseValueObject v = new HecErrorResponseValueObject();
        v.setCode(2);
        HecServerErrorResponseException ex = new HecServerErrorResponseException(v, b, EVENT_POST_OK, "my.url");
        Assert.assertEquals(ex.getErrorType(), HecServerErrorResponseException.Type.RECOVERABLE_CONFIG_ERROR);

        v.setCode(13);
        ex = new HecServerErrorResponseException(v, b,EVENT_POST_OK, "my.url");
        Assert.assertEquals(ex.getErrorType(), HecServerErrorResponseException.Type.RECOVERABLE_DATA_ERROR);
        
        v.setCode(6);
        ex = new HecServerErrorResponseException(v, b,EVENT_POST_OK, "my.url");
        Assert.assertEquals(ex.getErrorType(), HecServerErrorResponseException.Type.RECOVERABLE_DATA_ERROR);


        v.setCode(10);
        ex = new HecServerErrorResponseException(v,  b,EVENT_POST_OK, "my.url");
        Assert.assertEquals(ex.getErrorType(), HecServerErrorResponseException.Type.NON_RECOVERABLE_ERROR);

        v.setCode(8);
        ex = new HecServerErrorResponseException(v, b,EVENT_POST_OK, "my.url");
        Assert.assertEquals(ex.getErrorType(), HecServerErrorResponseException.Type.RECOVERABLE_SERVER_ERROR);
    }
}
