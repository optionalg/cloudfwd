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
package com.splunk.cloudfwd;

import com.splunk.cloudfwd.http.HttpEventCollectorSender;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author ghendrey
 */
public class ConfiguredObjectFactory {

  private static final Logger LOG = Logger.getLogger(
          ConfiguredObjectFactory.class.getName());

  private static final String TOKEN_KEY = "token";
  public static final String COLLECTOR_URI = "url";
  public static final String DISABLE_CERT_VALIDATION_KEY = "disableCertificateValidation";
  private static final String CHANNELS_PER_DESTINATION_KEY = "channels_per_dest";

  private Properties defaultProps = new Properties();

  public ConfiguredObjectFactory(Properties overrides) {
    this(); //setup all defaults by calling SenderFactory() empty constr
    this.defaultProps.putAll(overrides);
  }

  /**
   * create SenderFactory with default properties read from lb.properties file
   */
  public ConfiguredObjectFactory() {
    try {
      InputStream is = getClass().getResourceAsStream("/lb.properties");
      if (null == is) {
        throw new RuntimeException("can't find /lb.properties");
      }
      defaultProps.load(is);
    } catch (IOException ex) {
      LOG.log(Level.SEVERE, "problem loading lb.properties", ex);
      throw new RuntimeException(ex.getMessage(), ex);
    }
  }

  public List<URL> getUrls() {
    List<URL> urls = new ArrayList<>();
    String[] splits = defaultProps.getProperty(COLLECTOR_URI).split(",");
    for (String urlString : splits) {
      try {
        URL url = new URL(urlString.trim());
        urls.add(url);
      } catch (MalformedURLException ex) {
        Logger.getLogger(IndexDiscoverer.class.getName()).
                log(Level.SEVERE, "Malformed URL: '" + urlString + "'");
        Logger.getLogger(ConfiguredObjectFactory.class.getName()).log(
                Level.SEVERE, null,
                ex);
      }
    }
    return urls;
  }

  public int getChannelsPerDestination() {
    return Integer.parseInt(defaultProps.getProperty(
            CHANNELS_PER_DESTINATION_KEY, "8"));
  }

  public HttpEventCollectorSender createSender(URL url) {
    Properties props = new Properties(defaultProps);
    props.put("url", url.toString());
    return createSender(props);
  }

  private HttpEventCollectorSender createSender(Properties props) {
    try {
      String url = props.getProperty(COLLECTOR_URI).trim();
      String token = props.getProperty(TOKEN_KEY).trim();
      boolean disableCertificateValidation = Boolean.parseBoolean(props.
              getProperty(
                      DISABLE_CERT_VALIDATION_KEY, "false").trim());
      HttpEventCollectorSender sender = new HttpEventCollectorSender(url, token);
      if (disableCertificateValidation) {
        sender.disableCertificateValidation();
      }
      return sender;
    } catch (Exception ex) {
      LOG.log(Level.SEVERE, "Problem instantiating HTTP sender.", ex);
      throw new RuntimeException(
              "problem parsing lb.properties to create HttpEventCollectorSender",
              ex);
    }
  }

  public HttpEventCollectorSender createSender() {
    return createSender(this.defaultProps);
  }

}
