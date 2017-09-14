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

import com.splunk.cloudfwd.ConnectionSettings;
import com.splunk.cloudfwd.HecIllegalStateException;
import com.splunk.cloudfwd.impl.http.HttpSender;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;
import static com.splunk.cloudfwd.PropertyKeys.*;
/**
 *
 * @author ghendrey
 */
public class PropertiesFileHelper extends ConnectionSettings {



  public PropertiesFileHelper(Properties overrides) {
    this(); //setup all defaults by calling SenderFactory() empty constr
    this.defaultProps.putAll(overrides);
  }

  /**
   * create SenderFactory with default properties read from lb.properties file
   */
  public PropertiesFileHelper() {
    try {
      InputStream is = getClass().getResourceAsStream("/lb.properties");
      if (null == is) {
        throw new RuntimeException("can't find /lb.properties"); // TODO: This will be removed
      }
      defaultProps.load(is);
    } catch (IOException ex) {
      throw new HecIllegalStateException("Problem loading lb.properties", HecIllegalStateException.Type.CANNOT_LOAD_PROPERTIES);
    }
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
