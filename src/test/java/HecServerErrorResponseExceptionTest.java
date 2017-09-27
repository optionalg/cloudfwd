import com.splunk.cloudfwd.error.HecServerErrorResponseException;
import static com.splunk.cloudfwd.LifecycleEvent.Type.EVENT_POST_OK;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by mhora on 9/14/17.
 */
public class HecServerErrorResponseExceptionTest {

    @Test
    public void getError() {
        HecServerErrorResponseException ex = new HecServerErrorResponseException("my error message", 2, "", EVENT_POST_OK, "my.url");
        Assert.assertEquals(ex.getErrorType(), HecServerErrorResponseException.Type.RECOVERABLE_CONFIG_ERROR);

        ex = new HecServerErrorResponseException("my error message", 13, "",EVENT_POST_OK, "my.url");
        Assert.assertEquals(ex.getErrorType(), HecServerErrorResponseException.Type.RECOVERABLE_DATA_ERROR);

        ex = new HecServerErrorResponseException("my error message", 10, "",EVENT_POST_OK, "my.url");
        Assert.assertEquals(ex.getErrorType(), HecServerErrorResponseException.Type.NON_RECOVERABLE_ERROR);

        ex = new HecServerErrorResponseException("my error message", 8, "",EVENT_POST_OK, "my.url");
        Assert.assertEquals(ex.getErrorType(), HecServerErrorResponseException.Type.RECOVERABLE_SERVER_ERROR);
    }
}
