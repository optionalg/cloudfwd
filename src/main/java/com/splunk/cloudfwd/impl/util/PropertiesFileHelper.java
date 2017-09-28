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

    //FIXME TODO. THis needs to get OUT of the public API
  public HttpSender createSender(URL url, String host) {
    Properties props = new Properties(defaultProps);
    props.put(COLLECTOR_URI, url.toString());
    props.put(HOST, host.toString());
    return createSender(props);
  }

  private HttpSender createSender(Properties props) {
      // enable http client debugging
      if (enabledHttpDebug()) enableHttpDebug();
      String url = props.getProperty(COLLECTOR_URI).trim();
      String host = props.getProperty(HOST).trim();
      String token = props.getProperty(TOKEN).trim();
      String cert = getSSLCertContent();
      HttpSender sender = new HttpSender(url, token, isCertValidationDisabled(), cert, host);
      if(isMockHttp()){
        sender.setSimulatedEndpoints(getSimulatedEndpoints());
      }
      return sender;
  }

  //FIXME TODO. THis needs to get OUT of the public API
  public HttpSender createSender() {
    return createSender(this.defaultProps);
  }

}
