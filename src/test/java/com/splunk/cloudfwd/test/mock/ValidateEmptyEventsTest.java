package com.splunk.cloudfwd.test.mock;

import com.splunk.cloudfwd.*;

import org.junit.Test;
import org.slf4j.LoggerFactory;
import static org.junit.Assert.*;
import org.slf4j.Logger;


public class ValidateEmptyEventsTest {

    private static final Logger LOG = LoggerFactory.getLogger(ValidateEmptyEventsTest.class.getName());

    private byte[] bytes;
    private Exception ex = null;


    @Test
    public void TestWithAValidString() {
        try{
            LOG.info("Event with proper name");
            new EventWithMetadata("Event", 1);
        } catch (IllegalArgumentException e){
            ex = e;
        }
        assertNull(ex);
    }

    @Test
    public void TestWithEmptyString(){
        try{
            LOG.info("Empty event sent");
            new EventWithMetadata("", 2);
        }catch (IllegalArgumentException e){
            ex = e;
            LOG.info("Exception - empty event");
        }
        assertNotNull(ex);
    }

    @Test
    public void TestWithOnlySpaces(){
        try{
            LOG.info("Event with just the spaces");
            new EventWithMetadata("     ", 3);
        }catch (IllegalArgumentException e){
            ex = e;
            LOG.info("Exception - event with empty spaces");        }
        assertNotNull(ex);
    }

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

    @Test
    public void BytesArrayEmptyTest(){
        try{
            LOG.info("Empty byte array");
            new UnvalidatedBytesEvent(bytes,1);
        }catch (Exception e){
            ex = e;
            LOG.info("Exception - Empty byte array");
        }
        assertNotNull(ex);
    }

}

