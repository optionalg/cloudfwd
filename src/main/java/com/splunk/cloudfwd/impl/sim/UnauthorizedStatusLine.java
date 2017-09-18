package com.splunk.cloudfwd.impl.sim;

import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;

/**
 * Created by eprokop on 9/1/17.
 */
public class UnauthorizedStatusLine implements StatusLine {
    @Override
    public ProtocolVersion getProtocolVersion() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public int getStatusCode() {
        return 401;
    }

    @Override
    public String getReasonPhrase() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
