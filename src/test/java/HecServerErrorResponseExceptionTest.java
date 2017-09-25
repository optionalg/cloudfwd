import com.splunk.cloudfwd.HecServerErrorResponseException;
import com.splunk.cloudfwd.LifecycleEvent;
import static com.splunk.cloudfwd.LifecycleEvent.Type.PRE_ACK_POLL;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by mhora on 9/14/17.
 */
public class HecServerErrorResponseExceptionTest {

    @Test
    public void getError() {
        HecServerErrorResponseException ex = new HecServerErrorResponseException("my error message", 2, "",PRE_ACK_POLL, "my.url");
        Assert.assertEquals(ex.getErrorType(), HecServerErrorResponseException.Type.RECOVERABLE_CONFIG_ERROR);

        ex = new HecServerErrorResponseException("my error message", 13, "",PRE_ACK_POLL, "my.url");
        Assert.assertEquals(ex.getErrorType(), HecServerErrorResponseException.Type.RECOVERABLE_DATA_ERROR);

        ex = new HecServerErrorResponseException("my error message", 10, "",PRE_ACK_POLL, "my.url");
        Assert.assertEquals(ex.getErrorType(), HecServerErrorResponseException.Type.NON_RECOVERABLE_ERROR);

        ex = new HecServerErrorResponseException("my error message", 8, "",PRE_ACK_POLL, "my.url");
        Assert.assertEquals(ex.getErrorType(), HecServerErrorResponseException.Type.RECOVERABLE_SERVER_ERROR);
    }
}
