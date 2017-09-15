import com.splunk.cloudfwd.HecServerErrorResponseException;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by mhora on 9/14/17.
 */
public class HecServerErrorResponseExceptionTest {

    @Test
    public void getError() {
        HecServerErrorResponseException ex = new HecServerErrorResponseException("my error message", 2, "my.url");
        Assert.assertEquals(ex.getErrorType(), HecServerErrorResponseException.Type.RECOVERABLE_CONFIG_ERROR);

        ex = new HecServerErrorResponseException("my error message", 13, "my.url");
        Assert.assertEquals(ex.getErrorType(), HecServerErrorResponseException.Type.RECOVERABLE_DATA_ERROR);

        ex = new HecServerErrorResponseException("my error message", 9, "my.url");
        Assert.assertEquals(ex.getErrorType(), HecServerErrorResponseException.Type.NON_RECOVERABLE_ERROR);
    }
}
