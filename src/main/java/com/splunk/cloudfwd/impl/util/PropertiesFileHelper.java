/*
 * Copyright 2017 Splunk, Inc..
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.splunk.cloudfwd.impl.util;

import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.ConnectionSettings;
import com.splunk.cloudfwd.impl.http.HttpSender;

import java.net.URL;
import java.util.Properties;
import static com.splunk.cloudfwd.PropertyKeys.*;
import com.splunk.cloudfwd.error.HecConnectionStateException;
import org.apache.commons.lang.StringUtils;
import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
/**
 *
 * @author ghendrey
 */
public class PropertiesFileHelper extends ConnectionSettings {

  public PropertiesFileHelper(Connection c, Properties overrides) {
      super(c,overrides);
  }

  /**
   * create SenderFactory with default properties read from cloudfwd.properties file
   */
  public PropertiesFileHelper(Connection c) {
      super(c);
  }

  public boolean isForcedUrlMapToSingleAddr() {
    return Boolean.parseBoolean(this.defaultProps.getProperty(
            MOCK_FORCE_URL_MAP_TO_ONE, "false").trim());
  }

  /*
    //FIXME TODO. THis needs to get OUT of the public API
  public HttpSender createSender(URL url, String host) {
      
    this.connection.getSettings().setUrls(url.toString());
    // Use splunk_hec_host if set else use the hostname
    if (StringUtils.isEmpty(this.connection.getSettings().getHost())) {
      this.connection.getSettings().setHost(host);
    }
    return createSender();
  }
*/

  private HttpSender createSender(URL url, String sslHost) {
      // enable http client debugging
      if (enabledHttpDebug()) enableHttpDebug();
      String sslCert = getSSLCertContent();
      HttpSender sender = new HttpSender(url.toString(), sslHost, this, isCertValidationDisabled(), sslCert);
      if(isMockHttp()){
          sender.setSimulatedEndpoints(getSimulatedEndpoints());
      }
      return sender;
  }
  
    public HttpSender createSender(InetSocketAddress s) {
        try {
            //URLS for channel must be based on IP address not hostname since we
            //have many-to-one relationship between IP address and hostname via DNS records
            String hostAddr = s.getAddress().getHostAddress();
            if (s.getAddress() instanceof Inet6Address) {
                hostAddr = "[" + hostAddr + "]"; // java.net.URL requires braces for IPv6 host addresses
            }

            URL url = new URL("https://" + hostAddr + ":" + s.getPort());
            LOG.debug("Trying to add URL: " + url);
            //We should provide a hostname for http client, so it can properly set Host header
            //this host is required for many proxy server and virtual servers implementations
            //https://tools.ietf.org/html/rfc7230#section-5.4
            String host = s.getHostName() + ":" + s.getPort();

            return createSender(url, host);
        } catch (MalformedURLException ex) {
            LOG.error(ex.getMessage(), ex);
            throw new HecConnectionStateException(ex.getMessage(),
                    HecConnectionStateException.Type.CONFIGURATION_EXCEPTION);
        }
    }

}
