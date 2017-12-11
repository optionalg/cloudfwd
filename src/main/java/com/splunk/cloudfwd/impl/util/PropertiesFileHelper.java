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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.ConnectionSettings;
import com.splunk.cloudfwd.impl.ConnectionImpl;
import com.splunk.cloudfwd.impl.http.HttpSender;

import java.net.*;
import com.splunk.cloudfwd.error.HecConnectionStateException;

import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.util.Properties;

/**
 *
 * @author ghendrey
 */
public class PropertiesFileHelper extends ConnectionSettings {

    @JsonProperty("mock_force_url_map_to_one")
    private Boolean mockForceUrlMapToOne;

    public boolean isForcedUrlMapToSingleAddr() {
      return applyDefaultIfNull(this.mockForceUrlMapToOne, false);
    }

    public void setMockForceUrlMapToOne(Boolean force) {
        this.mockForceUrlMapToOne = force;
    }

  
    private HttpSender createSender(String url, String sslHost) {
      // enable http client debugging
      if (isHttpDebugEnabled()){
          setHttpDebugEnabled(true);
      }
      String sslCert = getSSLCertContent();
      HttpSender sender = new HttpSender(url, sslHost, this, isCertValidationDisabled(), sslCert);
      if(isMockHttp()){
        sender.setSimulatedEndpoints(getSimulatedEndpoints());
      }
      return sender;
  }
  
    public HttpSender createSender(InetSocketAddress s) {
        try {
            // this is to support the creation of channels for socket addresses that are not resolvable
            // so that they can get decomissioned and recreated at a later time, in case DNS recovers
            if (s.getAddress() == null) {
                return createSender(s.toString(), s.getHostName() + ":" + s.getPort());
            }            
            //URLS for channel must be based on IP address not hostname since we
            //have many-to-one relationship between IP address and hostname via DNS records
            String hostAddr = s.getAddress().getHostAddress();
            if (s.getAddress() instanceof Inet6Address) {
                hostAddr = "[" + hostAddr + "]"; // java.net.URL requires braces for IPv6 host addresses
            }

            URL url = new URL("https://" + hostAddr + ":" + s.getPort());
            getLog().debug("Trying to add URL: " + url);
            //We should provide a hostname for http client, so it can properly set Host header
            //this host is required for many proxy server and virtual servers implementations
            //https://tools.ietf.org/html/rfc7230#section-5.4
            String host = s.getHostName() + ":" + s.getPort();

            return createSender(url.toString(), host);
        } catch (MalformedURLException ex) {
            getLog().error(ex.getMessage(), ex);
            throw new HecConnectionStateException(ex.getMessage(),
                    HecConnectionStateException.Type.CONFIGURATION_EXCEPTION);
        }
    }

    @Override
    public void setConnection(Connection c) {
        connection = (ConnectionImpl)c;
        LOG = connection.getLogger(PropertiesFileHelper.class.getName());
    }

}
