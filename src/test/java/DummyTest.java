/*
 * Proprietary and confidential. Copyright Splunk 2015
 */
import com.splunk.logging.HttpEventCollectorErrorHandler;
import com.splunk.logging.HttpEventCollectorEventInfo;
import com.splunk.logging.SplunkCimLogEvent;
import java.util.List;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author ghendrey
 */
public class DummyTest {

  private static final Logger LOG = LogManager.getLogger("splunk.log4j");

  @Test
  public void testHec() throws InterruptedException {
    HttpEventCollectorErrorHandler.onError(
            new HttpEventCollectorErrorHandler.ErrorCallback() {
      public void error(final List<HttpEventCollectorEventInfo> data,
              final Exception ex) {
        System.out.println(ex);
      }
    });

    System.out.println("bork");
    SplunkCimLogEvent e = new SplunkCimLogEvent("Event name", "event-id") {
      {
        // You can add an arbitrary key=value pair with addField.
        addField("name", "value");

        // SplunkCimLogEvent provides lots of convenience methods for
        // fields defined by Splunk's Common Information Model. See
        // the SplunkCimLogEvent JavaDoc for a complete list.
        setAuthAction("deny");
      }
    };
    LOG.info(e);
    LOG.error("FPOOOOOO");
    LOG.error("bar");
    LOG.error("baz");
    LOG.error("zap");

    Thread.sleep(10000);

  }
}
