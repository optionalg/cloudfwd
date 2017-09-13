package com.splunk.cloudfwd;

/**
 * Created by mhora on 9/8/17.
 */
public class HecMissingPropertiesException extends RuntimeException{

    public HecMissingPropertiesException(String message) {
        super(message);
    }

}