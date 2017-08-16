package com.splunk.cloudfwd.http;

import org.apache.http.*;

/**
 * Created by eprokop on 8/9/17.
 */
public class ElbCookie {
    public static final String COOKIE_NAME = "AWSELB";
    private String value = null;

    public ElbCookie(HttpResponse response) {
        HeaderIterator headerIterator = response.headerIterator("Set-Cookie");
        while (headerIterator.hasNext()) {
            Header header = (Header)headerIterator.next();
            HeaderElement[] elements = header.getElements();
            for (HeaderElement element : elements) {
                if (element.getName().equals(COOKIE_NAME)) {
                    value = element.getValue();
                    break;
                }
            }
            if (value != null) break;
        }
    }

    /**
     *
     * @return the value of the cookie (or null if there was no AWSELB cookie in the response)
     */
    public String getValue() {
        return value;
    }

    /**
     *
     * @return the name and value pair for the cookie to be used in an HTTP header
     */
    public String getNameValuePair() {
        if (value != null) {
            return COOKIE_NAME + "=" + value;
        }
        return "";
    }

    @Override
    public String toString() {
        return getNameValuePair();
    }

}
