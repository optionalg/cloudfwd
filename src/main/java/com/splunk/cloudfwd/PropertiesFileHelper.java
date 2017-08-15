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

import com.splunk.cloudfwd.http.Endpoints;
import com.splunk.cloudfwd.http.HttpEventCollectorSender;
import com.splunk.cloudfwd.sim.SimulatedHECEndpoints;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author ghendrey
 */
public class PropertiesFileHelper {

  private static final Logger LOG = Logger.getLogger(PropertiesFileHelper.class.
          getName());

  public static final String TOKEN_KEY = "token";
  public static final String COLLECTOR_URI = "url";
  public static final String DISABLE_CERT_VALIDATION_KEY = "disableCertificateValidation";
  public static final String CHANNELS_PER_DESTINATION_KEY = "channels_per_dest";
  public static final String MOCK_HTTP_KEY = "mock_http";
  public static final String MOCK_HTTP_CLASSNAME_KEY = "mock_http_classname";

  private Properties defaultProps = new Properties();
  private Connection connection;

  public PropertiesFileHelper(Connection connection, Properties overrides) {
    this(); //setup all defaults by calling SenderFactory() empty constr
    this.defaultProps.putAll(overrides);
    this.connection = connection;
  }

  /**
   * create SenderFactory with default properties read from lb.properties file
   */
  public PropertiesFileHelper() {
    try {
      InputStream is = getClass().getResourceAsStream("/lb.properties");
      if (null == is) {
        throw new RuntimeException("can't find /lb.properties");
      }
      defaultProps.load(is);
    } catch (IOException e) {
      LOG.severe("problem loading lb.properties");
      connection.getCallbacks().failed(null, e);
    }
  }

  public List<URL> getUrls() {
    List<URL> urls = new ArrayList<>();
    String[] splits = defaultProps.getProperty(COLLECTOR_URI).split(",");
    for (String urlString : splits) {
      try {
        URL url = new URL(urlString.trim());
        urls.add(url);
      } catch (MalformedURLException e) {
        LOG.severe(e.getMessage());
        Logger.getLogger(IndexDiscoverer.class.getName()).
                log(Level.SEVERE, "Malformed URL: '" + urlString + "'");
        this.connection.getCallbacks().failed(null, e);

      }
    }
    return urls;
  }

  public int getChannelsPerDestination() {
    return Integer.parseInt(defaultProps.getProperty(
            CHANNELS_PER_DESTINATION_KEY, "8"));
  }

  public boolean isMockHttp() {
    return Boolean.parseBoolean(this.defaultProps.getProperty(MOCK_HTTP_KEY,
            "false").trim());
  }
  
  public Endpoints getSimulatedEndpoints(){
    String classname = this.defaultProps.getProperty(MOCK_HTTP_CLASSNAME_KEY,"com.splunk.cloudfwd.sim.SimulatedHECEndpoints");
    try {
      return (Endpoints) Class.forName(classname).newInstance();
    } catch (Exception e) {
      LOG.severe(e.getMessage());
      this.connection.getCallbacks().failed(null, e);
      throw new RuntimeException(e.getMessage(), e); // Method must return Endpoints value/throw exception
    }
    
  } 

  public boolean isCertValidationDisabled() {
    return Boolean.parseBoolean(this.defaultProps.
            getProperty(
                    DISABLE_CERT_VALIDATION_KEY, "false").trim());

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
      HttpEventCollectorSender sender = new HttpEventCollectorSender(url, token);
      if (isCertValidationDisabled()) {
        sender.disableCertificateValidation();
      }
      if(isMockHttp()){
        sender.setSimulatedEndpoints(getSimulatedEndpoints());
      }
      return sender;
    } catch (Exception e) {
      String msg = "problem parsing lb.properties to instantiate HttpEventCollectorSender.";
      LOG.severe(msg);
      this.connection.getCallbacks().failed(null, e);
      throw new RuntimeException(msg); // Method must return HttpEventCollectorSender value / throw Exception
    }
  }

  public HttpEventCollectorSender createSender() {
    return createSender(this.defaultProps);
  }

}
