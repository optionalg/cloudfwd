package mock_tests.in_detention_tests;

import com.splunk.cloudfwd.*;
import com.splunk.cloudfwd.impl.util.PropertiesFileHelper;
import mock_tests.in_detention_tests.AbstractInDetentionTest;
import test_utils.BasicCallbacks;
import com.splunk.cloudfwd.error.HecServerErrorResponseException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

import static com.splunk.cloudfwd.PropertyKeys.BLOCKING_TIMEOUT_MS;
import static com.splunk.cloudfwd.PropertyKeys.MOCK_HTTP_CLASSNAME;

/**
 * Created by mhora on 10/4/17.
 */
public class InDetentionAllTest extends AbstractInDetentionTest {

    protected BasicCallbacks getCallbacks() {
        return new BasicCallbacks(getNumEventsToSend()) {

            @Override
            protected boolean isExpectedWarningType(Exception e) {
                return (e instanceof HecServerErrorResponseException &&
                        ((HecServerErrorResponseException)e).getLifecycleType()==LifecycleEvent.Type.INDEXER_IN_DETENTION);
            }

            @Override
            public boolean shouldWarn(){
                return true; //each failed preflight test will return INDEXER_IN_DETENTION via a systemWarning callback
            }

        };
    }

    @Override
    protected void setProps(PropertiesFileHelper settings) {
        settings.setMockHttpClassname("com.splunk.cloudfwd.impl.sim.errorgen.indexer.InDetentionEndpoints");
        settings.setBlockingTimeoutMS(30000);
        settings.setUnresponsiveMS(-1); //no dead channel detection
        settings.setMaxTotalChannels(2);
    }

    protected void createConnection(LifecycleEvent.Type problemType) {
        PropertiesFileHelper settings = this.getTestProps();
        this.setProps(settings);
        boolean gotException = false;
        try{
            this.connection = Connections.create((ConnectionCallbacks) callbacks, settings);
        }catch(Exception e){
            Assert.assertTrue("Expected HecServerErrorResponseException",  e instanceof HecServerErrorResponseException);
            HecServerErrorResponseException servRespExc = (HecServerErrorResponseException) e;
            Assert.assertTrue("HecServerErrorResponseException not "+problemType+", was  " + servRespExc.getLifecycleType(),
                    servRespExc.getLifecycleType()==problemType);
            gotException = true;
        }
        if(!gotException){
            Assert.fail("Expected HecMaxRetriedException associated with Connection instantiation config checks'");
        }
    }

    @Override
    protected boolean shouldSendThrowException() { //fixme todo - it ain't even gonna get to send. It will fail fast instantiating connection
        return true;
    }

    @Test
    public void sendToIndexersInDetention() throws InterruptedException {
        createConnection(LifecycleEvent.Type.INDEXER_IN_DETENTION);
    }
}
