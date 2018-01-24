package com.splunk.cloudfwd.test.mock;

import com.splunk.cloudfwd.EventWithMetadata;
import com.splunk.cloudfwd.RawEvent;
import com.splunk.cloudfwd.UnvalidatedByteBufferEvent;
import com.splunk.cloudfwd.UnvalidatedBytesEvent;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class ValidateEmptyEventsTest {
    private static final Logger LOG = LoggerFactory.getLogger(ValidateEmptyEventsTest.class.getName());
    private Exception ex = null;
    private ByteBuffer emptyByteBuffer = ByteBuffer.allocate(0);
    private byte[] emptyByteArray = new byte[0];


    /**
     *This test is used to check if the event has a valid name.
     */
    @Test
    public void TestWithAValidString(){
        try {
            LOG.info("Event with proper name");
            new EventWithMetadata("Event", 1);
        } catch (IllegalArgumentException e){
            ex = e;
        }
        assertNull(ex);
    }

    /**
     * This test is used to detect events which have an empty string as the name of the event.
     */
    @Test
    public void TestWithEmptyString(){
        try {
            LOG.info("Empty event sent");
            new EventWithMetadata("", 2);
        } catch (IllegalArgumentException e){
            ex = e;
            LOG.info("Exception - empty event");
        }
        assertNotNull(ex);
    }


    /**
     * This test is used to detect events where the name of the event is null.
     */
    @Test
    public void TestWithEventAsNull(){
        try{
            LOG.info("Null event");
            new EventWithMetadata(null, 4);
        }catch (IllegalArgumentException e){
            ex = e;
            LOG.info("Exception - null event");
        }
        assertNotNull(ex);
    }

    /**
     * This test is used to detect events which have just blank spaces as the name of the event.
     */
    @Test
    public void TestWithOnlySpaces(){
        try{
            LOG.info("Event with just empty spaces");
            new EventWithMetadata("   ", 4);
        }catch (IllegalArgumentException e){
            ex = e;
            LOG.info("Exception - event with empty spaces");
        }
        assertNotNull(ex);
    }


    @Test
    public void TestWithEmptyByteBuffer(){
        try{
            LOG.info("Passing in an empty ByteBuffer");
            new UnvalidatedByteBufferEvent(emptyByteBuffer,1);
        }catch (IllegalArgumentException e){
            ex = e;
            LOG.info("Exception - ByteBuffer Empty");
        }
        assertNotNull(ex);
    }

    @Test
    public void TestWithEmptyByteArray(){
        try{
            LOG.info("Passing in an empty ByteArray ");
            new UnvalidatedBytesEvent(emptyByteArray,1);
        }catch (IllegalArgumentException e){
            ex = e;
            LOG.info("Exception - Empty Byte Array");
        }
        assertNotNull(ex);
    }

    @Test
    public void TestWithEmptyRawEvent() {
        try {
            LOG.info("Empty string as a raw event");
            RawEvent.fromText("", 1);
        }catch (IllegalArgumentException e){
            ex = e;
            LOG.info("Exception - Empty raw event");
        }
        assertNotNull(ex);
    }

}
