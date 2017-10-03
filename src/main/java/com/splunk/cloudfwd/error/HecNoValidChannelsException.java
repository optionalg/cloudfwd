package com.splunk.cloudfwd.error;

import com.splunk.cloudfwd.HecHealth;

import java.util.List;

/**
 * Created by eprokop on 10/3/17.
 */
public class HecNoValidChannelsException extends RuntimeException {
    private List<HecHealth> hecHealthList;

    public HecNoValidChannelsException(String msg, List<HecHealth> hecHealthList) {
        super(msg);
        this.hecHealthList = hecHealthList;
    }

    public List<HecHealth> getHecHealthList() {
        return hecHealthList;
    }
}
