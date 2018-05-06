package com.splunk.cloudfwd.impl.http;

import com.splunk.cloudfwd.impl.util.HecChannel;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.slf4j.Logger;

import java.net.HttpCookie;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Stores Session Cookie 
 */
public class ChannelCookies implements Cloneable {
  
  private final Logger LOG; 
  private List<HttpCookie> cookies = new ArrayList<>();
  private HecChannel channel;
  
  private static final String PATH_NAME = "PATH"; 
  private static final String MAX_AGE_NAME = "MAX-AGE";
  
  /**
   * Session Cookie object. 
   * 
   * Example ELB Set-Cookie header
   * Set-Cookie: AWSELB=633765DB082FD002198A92E31DFFD5BDEA5A74557F84FE2BE8A8B6F6EEFD920BEF75DDED44CED67DBEBCCEB6B8164F3FE2216A8454BB86B94FD977DDF4E76E95766567589D;PATH=/;MAX-AGE=1
   */
  public ChannelCookies(HecChannel channel, HttpResponse response) {
    this.LOG = channel.getConnection().getLogger(ChannelCookies.class.getName());
    Header[] headers = response.getHeaders("Set-Cookie");
    try {
      for (Header header: headers) {
        cookies.addAll(HttpCookie.parse(header.toString()));
      }
    } catch (Exception e) {
      LOG.error("channel={} headers={} cookies={}", channel, headers, cookies);
      throw e;
    }
    LOG.debug("channel={} headers={} cookies={}", channel, headers, cookies);
  }
  
  public boolean isEmpty(){ 
    return this.cookies == null || this.cookies.isEmpty();
  }
  
  public boolean equals(ChannelCookies cookies1) {
    return this.toString().equalsIgnoreCase(cookies1.toString());
  }
  
  public String toString() {
    return getCookieHeader();
  }
  
  /**
   * Checks if Expiration or MAX-AGE is set
   */
  public boolean isExpirationSet() {
      for (HttpCookie cookie: cookies) {
        LOG.debug("ChannelCookies: isExpirationSet: cookie_max_age={}", cookie.getMaxAge());
        if (cookie.getMaxAge() >= 0 ) {
          return true;
        }
      }
      return false;
  }
  
  public String getCookieHeader() {
    if (cookies == null) {
      return null;
    }
    List<String> cookie_headers = new ArrayList<>(); 
    StringBuffer sb = new StringBuffer();
    for (HttpCookie cookie: cookies) {
      String cookie_header = cookie.getName() + "=" + cookie.getValue() +
              ";" + PATH_NAME + "=" + cookie.getPath();
      if(cookie.getMaxAge() >= 0) {
        cookie_header = cookie_header + ";" + MAX_AGE_NAME + "=" + cookie.getMaxAge();
      }
      cookie_headers.add(cookie_header);
    }
    return String.join(",", cookie_headers);
  }
}
