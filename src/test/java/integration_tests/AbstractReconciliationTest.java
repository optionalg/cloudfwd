package integration_tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.Event;
import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.error.HecConnectionTimeoutException;
import com.splunk.cloudfwd.impl.http.HttpClientFactory;
import test_utils.AbstractConnectionTest;
import java.io.IOException;
import java.util.*;

import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract class for testing that events sent via the cloudfwd library are indexed
 * and searchable as expected in Splunk. Tests extending AbstractReconciliationTest
 * require a Splunk single instance to run against. It is NOT RECOMMENDED to run these tests
 * against a production Splunk instance. Tests may do any of the following:
 *  - Index data
 *  - Create and delete an index
 *  - Create and delete an Http Event Collector token
 *  - Create and query search jobs
 *  - Install or update Splunk apps or add-ons
 *
 * To run, enter configuration for a Splunk instance under "CONFIGURABLE" below.
 *
 * To run tests from CLI (with different configurations)
 * EG: mvn test -Dtest=AWSSourcetypeIT "-DargLine=-Duser=admin -Dpassword=changeme -DsplunkHost=localhost -DmgmtPort=8089"
 */
public abstract class AbstractReconciliationTest extends AbstractConnectionTest {

  protected static final Logger LOG = LoggerFactory.getLogger(AbstractReconciliationTest.class.getName());

  /* ************ /CONFIGURABLE ************ */
  protected int numToSend = 10;
  private final Boolean ENABLE_TEST_HTTP_DEBUG = false; // Enable HTTP debug in test client
  private String TOKEN_NAME; // per-test generated HEC Token Name
  private String TOKEN_VALUE = null; // per-test generated HEC Token
  protected CloseableHttpClient httpClient; // per test class httpClient shared across tests
  protected String INDEX_NAME; // per-test generated index name
  protected String[] EXPECTED_FIELDS = null; // per-test list of field names that search
  protected ObjectMapper json = new ObjectMapper();
  // results should contain based on props.conf entries.

  // enable HEC only once per class run. Has to be a class variable, as each
  // junit test instantiate a new instance of the test class
  private static Boolean HEC_ENABLED = false;
  /* ************ CLI CONFIGURABLE ************ */
  private static Map<String, String> cliProperties;
  // Default values
  static {
    cliProperties = new HashMap<>();
    cliProperties.put("splunkHost", "localhost");
    cliProperties.put("mgmtPort", "8089");
    cliProperties.put("user", "admin");
    cliProperties.put("password", "changeme");
  }

  public AbstractReconciliationTest() {
    super();
    LOG.info("NEXT RECONCILIATION TEST...");
    // Get any command line arguments
    cliProperties = getCliTestProperties();
    // Build a client to share among tests
    httpClient = buildSplunkClient();
    if (ENABLE_TEST_HTTP_DEBUG) enableTestHttpDebug();
  }

  @Before
  public void init() {
    if (!HEC_ENABLED) {
      enableHec();
    }
  }

  @After
  public void tearDown(){
    deleteTestToken();
    deleteTestIndex();
      try {
          httpClient.close();
      } catch (IOException ex) {
          LOG.error("Error closing connection used by tests to setup splunk",ex);
          Assert.fail(ex.getMessage());
      }
  }

  @Override
  protected Properties getProps() {
    Properties props = new Properties();
    props.put(PropertyKeys.MOCK_HTTP_KEY, "false");
    props.put(PropertyKeys.EVENT_BATCH_SIZE, "16000");
    return props;
  }

  @Override
  protected boolean shouldCacheEvents() {
    return true; //so that we record all sent events for later comparison with retrieved events
  }

  protected String mgmtSplunkUrl() {
    return "https://" + cliProperties.get("splunkHost") + ":" + cliProperties.get("mgmtPort");
  }

  /*
  Get command line arguments for Test
   */
  protected Map<String, String> getCliTestProperties() {
    if (System.getProperty("argLine") != null) {
      LOG.warn("Replacing test properties with command line arguments");
      Set<String> keys = cliProperties.keySet();
      for (String e : keys) {
        if (System.getProperty(e) != null) {
          cliProperties.replace(e, System.getProperty(e));
        }
      }
    }
    LOG.warn("Test Arguments:" + cliProperties);
    return cliProperties;
  }

  /*
   * Builds a Splunk Client for REST Mgmt.
   */
  protected CloseableHttpClient buildSplunkClient() {
    CloseableHttpClient httpClient = null;
    try {
      // credentials
      CredentialsProvider credsProvider = new BasicCredentialsProvider();
      credsProvider.setCredentials(new AuthScope(cliProperties.get("splunkHost"),
                      new Integer(cliProperties.get("mgmtPort"))),
              new UsernamePasswordCredentials(cliProperties.get("user"), cliProperties.get("password")));
      // create synchronous http client that ignores SSL
      httpClient = HttpClientBuilder.create().
              setDefaultCredentialsProvider(credsProvider).
              setHostnameVerifier(
                      SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER).
              setSSLContext(HttpClientFactory.build_ssl_context_allow_all()).
              build();
    } catch (Exception ex) {
      Assert.fail("Problem Building Splunk Client, ex: " +
              ex.getMessage());
    }
    return httpClient;
  }

  /*
   * Searches Splunk for events that were sent to the index
   * created for the test.
   */
  protected Set<String> getEventsFromSplunk() {
    // Give events a few seconds to be indexed so they show up in search.
    // Even after acks are received, there is a lag before events are searchable
    // Unfortunately there is no programmatically deterministic way of waiting for this, so we sleep.
    sleep(3000);
    Set<String> results = new HashSet<>();
    try {
      // credentials
      HttpClient httpClient = buildSplunkClient();
      String sid = createSearchJob(httpClient);
      results = queryJobForResults(httpClient, sid);
    } catch (Exception e) {
      Assert.fail("Problem getting search results from Splunk: " +
              e.getMessage());
    }
    return results;
  }

  protected String getSearchString() {
    // index=<test-index-name>
    return "search index=" + INDEX_NAME;
  }

  /*
   * Hits Splunk REST API to create a new search job and
   * returns the search id of the job.
   */
  protected String createSearchJob(HttpClient httpClient) throws IOException {
    // POST to create a new search job
    HttpPost httpPost = new HttpPost(
            mgmtSplunkUrl() + "/services/search/jobs");
    List<NameValuePair> params = new ArrayList<>();
    params.add(new BasicNameValuePair("output_mode", "json"));
    params.add(new BasicNameValuePair("exec_mode", "blocking")); // splunk won't respond until search is complete
    params.add(new BasicNameValuePair("search", getSearchString()));
    params.add(new BasicNameValuePair("earliest_time", "1")); // all-time search (OK since index is small)
    params.add(new BasicNameValuePair("latest_time", "now"));
    params.add(new BasicNameValuePair("adhoc_search_level", "smart")); // extracts fields
    httpPost.setEntity(new UrlEncodedFormEntity(params, "UTF-8"));
    //Execute and get the response.
    HttpResponse postResponse = httpClient.execute(httpPost);
    String reply = parseHttpResponse(postResponse);
    return json.readTree(reply).path("sid").asText();
  }

  protected void restartSplunk() {
    try {
      enableTestHttpDebug();
      HttpPost httpPost = new HttpPost(mgmtSplunkUrl() +
              "/services/server/control/restart");
      HttpResponse httpResponse = httpClient.execute(httpPost);
      parseHttpResponse(httpResponse);
      LOG.info("restartSplunk: Splunk successfully restarted");
    } catch (Exception ex) {
      Assert.fail("restartSplunk: Failed to restart Splunk: " +
              ex.getMessage());
    }
  }

  /*
   * Checks HTTP request for errors. If no errors, returns the response as a String.
   */
  protected String parseHttpResponse(HttpResponse httpResponse) throws IOException {
    String reply;
    if (httpResponse.getStatusLine().getStatusCode() >= 400) {
      LOG.error("checkHttpResponseOrFail: httpResponse" + httpResponse);
      LOG.error("checkHttpResponseOrFail: httpResponse.getStatusLine().getStatusCode(): " +
              httpResponse.getStatusLine().getStatusCode());
      reply = EntityUtils.toString(httpResponse.getEntity(), "utf-8");
      if (reply.toLowerCase().contains("unauthorized")) {
        throw new RuntimeException(reply);
      }
      Assert.fail("Http request failed. Got error httpResponse: " +
        httpResponse);
    }
    return EntityUtils.toString(httpResponse.getEntity(), "utf-8");
  }

  protected void deleteTestToken() {
    if (TOKEN_VALUE != null) {
      try {
        HttpDelete httpRequest = new HttpDelete(mgmtSplunkUrl() +
                "/services/data/inputs/http/" + TOKEN_NAME);
        HttpResponse httpResponse = httpClient.execute(httpRequest);
        parseHttpResponse(httpResponse);
        LOG.debug("deleteTestToken: httpResponse: " + httpResponse);
        LOG.info("deleteTestToken: Successfully deleted token: TOKEN_NAME=" +
                TOKEN_NAME + " TOKEN_VALUE=" + TOKEN_VALUE);
        TOKEN_VALUE = null;
      } catch (Exception ex) {
        Assert.fail("deleteTestToken: failed with ex: " + ex.getMessage());
      }
    } else {
      LOG.warn("Skipped deleting test token since test didn't create a token.");
    }
  }

  private void createTestIndex() {
    if (INDEX_NAME != null) deleteTestIndex();
    INDEX_NAME = java.util.UUID.randomUUID().toString();
    try {
      HttpPost httpPost = new HttpPost(mgmtSplunkUrl() +
              "/services/data/indexes/");
      List<NameValuePair> params = new ArrayList<>();
      params.add(new BasicNameValuePair("name", INDEX_NAME));
      params.add(new BasicNameValuePair("output_mode", "json"));
      httpPost.setEntity(new UrlEncodedFormEntity(params, "UTF-8"));
      HttpResponse httpResponse = httpClient.execute(httpPost);
      parseHttpResponse(httpResponse);
      LOG.debug("createTestIndex: httpResponse: " + httpResponse);
      LOG.info("createTestIndex: Index successfully created. INDEX_NAME=" + INDEX_NAME);
    } catch (Exception ex) {
      Assert.fail("createTestIndex: Failed to create index: " +
        ex.getMessage());
    }
  }

  private void deleteTestIndex() {
    if (INDEX_NAME != null) {
      try {
        HttpDelete httpRequest = new HttpDelete(mgmtSplunkUrl() +
                "/services/data/indexes/" + INDEX_NAME);
        HttpResponse httpResponse = httpClient.execute(httpRequest);
        parseHttpResponse(httpResponse);
        LOG.debug("deleteTestIndex: httpResponse: " + httpResponse);
        LOG.info("deleteTestIndex: Successfully deleted index. INDEX_NAME=" + INDEX_NAME);
      } catch (Exception ex) {
        Assert.fail("deleteTestIndex: failed to delete index: " + ex.getMessage());
      }
    } else {
      LOG.warn("Skipped deleting test index since test didn't create an index.");
    }
  }

  protected void enableHec() {
    try {
      HttpPost httpRequest = new HttpPost(mgmtSplunkUrl() +
              "/services/data/inputs/http/http");
      List<NameValuePair> params = new ArrayList<>();
      params.add(new BasicNameValuePair("disabled", "0"));
      params.add(new BasicNameValuePair("output_mode", "json"));
      httpRequest.setEntity(new UrlEncodedFormEntity(params, "UTF-8"));

      HttpResponse httpResponse = httpClient.execute(httpRequest);
      String httpReply = parseHttpResponse(httpResponse);

      // Parse json from http reply
      JsonNode json_reply = json.readTree(httpReply);

      // Check if we received an error message
      if (json_reply.findValue("messages").asBoolean()) {
        JsonNode message = json_reply.findValue("messages");
        LOG.info("DEBUG: message: " + message);
        if (message.findValue("type").asText().equals("ERROR")) {
          Assert.fail("enableHec: Failed to enable HEC. Server returned message: " +
                  message.asText());
        }
      }

      // Parse the response to find token id
      JsonNode entry = json_reply.path("entry").get(0);
      String port = entry.path("content").path("port").asText();
      this.HEC_ENABLED = true;
      LOG.info("enableHec: Successfully enabled HEC on port " + port);
    } catch (Exception ex) {
      Assert.fail("enableHec: Failed to enable HEC, ex: " +
              ex.getMessage());
    }
  }

  protected String createTestToken(String sourcetype) {
    return createTestToken(sourcetype, true);
  }

  // pass sourcetype=null to use the token default sourcetype
  protected String createTestToken(String sourcetype, boolean useACK) {
    if (TOKEN_VALUE != null) deleteTestToken();
    createTestIndex();
    TOKEN_NAME = java.util.UUID.randomUUID().toString();
    try {
      HttpPost httpPost = new HttpPost(mgmtSplunkUrl() +
              "/services/data/inputs/http/");
      List<NameValuePair> params = new ArrayList<>();
      params.add(new BasicNameValuePair("name", TOKEN_NAME));
      if (sourcetype != null) {
        params.add(new BasicNameValuePair("sourcetype", sourcetype));
      }
      params.add(new BasicNameValuePair("index", INDEX_NAME));
      String useACKValue = useACK ? "1" : "0";
      params.add(new BasicNameValuePair("useACK", useACKValue));
      params.add(new BasicNameValuePair("output_mode", "json"));
      httpPost.setEntity(new UrlEncodedFormEntity(params, "UTF-8"));
      HttpResponse httpResponse = httpClient.execute(httpPost);
      String httpReply = parseHttpResponse(httpResponse);

      // Parse json from http reply
      JsonNode json_reply = json.readTree(httpReply);

      // Check if we received an error message
      if (json_reply.findValue("messages").asBoolean()) {
        JsonNode message = json_reply.findValue("messages");
        if (message.findValue("type").asText().equals("ERROR")) {
          Assert.fail("createTestToken: Failed to create token: TOKEN_NAME=" +
                  TOKEN_NAME + ", server returned error message: " +
                  message.asText());
        }
      }

      // Parse the response to find token id
      JsonNode entry = json_reply.path("entry").get(0);
      TOKEN_VALUE = entry.path("content").path("token").asText();
      LOG.info("createTestToken: Successfully created token. TOKEN_VALUE=" + TOKEN_VALUE);
    } catch (Exception ex) {
      Assert.fail("createTestToken: Failed to create token, ex: " +
              ex.getMessage());
    }
    return TOKEN_VALUE;
  }

  protected void verifyResults(List<Event> sentEvents, Set<String> searchResults) {
    LOG.info("Verifying results...");
    Set<String> searchResultsCopy = new HashSet<>(searchResults); // don't modify the original set of results
    if (sentEvents.size() > 100) {
      throw new RuntimeException(
              "Splunk Search Endpoint can't return more than 100 results at once. "
                      + "And this test is lazy - it will not paginate over many pages of results to verify. "
                      + "So be nice and limit your reconciliation test to 100 events. Or go make this test better with pagination.");
    }
    if (sentEvents.size() != searchResultsCopy.size()) {
      Assert.fail(
              "Number of events sent and search results do not match: events_sent=" + sentEvents.
              size() + " search_results=" + searchResultsCopy.size());
    }
    for (Event e : sentEvents) {
      String eventText = null;
      if (isEventEndpoint()) {
        // extract the event text from the "event" key
        try {
          // "event" key contains raw text
          eventText = json.readTree(e.toString()).path("event").asText();
          if (eventText.isEmpty()) {
            // "event" key contains JSON
            eventText = json.readTree(e.toString()).path("event").toString();
          }
        } catch (IOException e1) {
          Assert.fail(
                  "Event was: " + e + ". Could not parse 'event' key from JSON object: " + e1.
                  getMessage());
        }
      } else {
        eventText = e.toString();
      }
      //Check that the retrieved event text exactly matched sent event text
      if (!searchResultsCopy.remove(eventText.trim())) {
        Assert.fail("Event was not present in search results: " + e.toString());
      } else {
        LOG.trace("Validated event");
      }
    }
    if (searchResultsCopy.size() != 0) {
      Assert.fail(
              "Search returned " + searchResultsCopy.size() + " more events than were sent.");
    }
    LOG.info("Results OK. Splunk search returned all expected events.");
  }

  /*
   * Queries Splunk REST API with a job search id to
   * return raw event text of the search results.
   */
  private Set<String> queryJobForResults(HttpClient httpClient, String sid)
          throws IOException {
    Set<String> results = new HashSet<>();
    HttpGet httpget = new HttpGet(
            "https://" + cliProperties.get("splunkHost") + ":" + cliProperties.get("mgmtPort")
            + "/services/search/jobs/" + sid + "/results?output_mode=json");

    HttpResponse getResponse = httpClient.execute(httpget);
    String getReply = parseHttpResponse(getResponse);

    JsonNode n = json.readTree(getReply);
    if (getReply.toLowerCase().contains("unauthorized")) {
      throw new RuntimeException(getReply);
    }
    for (JsonNode eventNode : n.path("results")) {
      if (this.EXPECTED_FIELDS != null) verifyFields(eventNode);
      boolean success = results.add(eventNode.path("_raw").asText());
      if (!success) {
        throw new RuntimeException("Events sent to Splunk were not unique.");
      }
    }
    return results;
  }

  /*
   * Makes sure that the event contains all of the expected fields.
   */
  private void verifyFields(JsonNode eventNode) {
    if (this.EXPECTED_FIELDS == null) {
      throw new RuntimeException("Must specify field names to verify against.");
    }
    for (String field : EXPECTED_FIELDS) {
      if (eventNode.get(field) == null || eventNode.get(field).asText().isEmpty()) {
        Assert.fail("Field value not present in event: " +
          "field=" + field + " event=" + eventNode.get("_raw").asText());
      }
    }
  }

  private boolean isEventEndpoint() {
    return connection.getSettings().getHecEndpointType().equals(Connection.HecEndpoint.STRUCTURED_EVENTS_ENDPOINT);
  }

  private void enableTestHttpDebug() {
    System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.SimpleLog");
    System.setProperty("org.apache.commons.logging.simplelog.showdatetime", "true");
    System.setProperty("org.apache.commons.logging.simplelog.log.httpclient.wire.header", "debug");
    System.setProperty("org.apache.commons.logging.simplelog.log.org.apache.http", "debug");
    LOG.info("Enabled Test HTTP Debug");
  }

  protected String getTokenValue() {
    return TOKEN_VALUE;
  }

  protected void sleep(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException ex) {
      throw new RuntimeException(ex.getMessage(), ex);
    }
  }

}
