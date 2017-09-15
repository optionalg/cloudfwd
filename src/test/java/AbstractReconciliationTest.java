
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.Event;
import com.splunk.cloudfwd.PropertyKeys;
import com.splunk.cloudfwd.impl.http.HttpClientFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public abstract class AbstractReconciliationTest extends AbstractConnectionTest {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractReconciliationTest.class.getName());

  /* ************ CONFIGURABLE ************ */
  protected int numToSend = 10;
  // change these settings based on the Splunk search head you want the test to search on:
  private String splunkHost = "localhost";
  private String mgmtPort = "8089"; // management port on the Splunk search head
  private String index = "firehose"; // where the data is indexed – this is the index that will be searched
  private String user = "admin"; // a Splunk user that has permissions to search in <index>
  private String password = "changeme";

  /* ************ /CONFIGURABLE ************ */
  public AbstractReconciliationTest() {
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

  protected Set<String> getEventsFromSplunk() {
    Set<String> results = new HashSet<>();
    try {
      // credentials
      CredentialsProvider credsProvider = new BasicCredentialsProvider();
      credsProvider.setCredentials(new AuthScope(splunkHost,
              new Integer(mgmtPort)),
              new UsernamePasswordCredentials(user, password));
      // create synchronous http client that ignores SSL
      HttpClient httpClient = HttpClientBuilder.create().
              setDefaultCredentialsProvider(credsProvider).
              setHostnameVerifier(
                      SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER).
              setSSLContext(HttpClientFactory.build_ssl_context_allow_all()).
              build();
      String sid = createSearchJob(httpClient);
      results = queryJobForResults(httpClient, sid);
    } catch (Exception e) {
      Assert.fail("Problem getting search results from Splunk: " + e.
              getMessage());
    }
    return results;
  }

  protected String getSearchString() {
    // index=<index> | extract kvdelim="=" | search testMethodGUID=<testMethodGUID>
    return new StringBuilder().append("search index=").append(index).
            append(" | extract kvdelim=\"=\" | search ").
            append(TEST_METHOD_GUID_KEY).append("=").append(testMethodGUID).
            toString();
  }

  /*
   * Hits Splunk REST API to create a new search job and
   * returns the search id of the job.
   */
  protected String createSearchJob(HttpClient httpClient) throws IOException {
    // POST to create a new search job
    HttpPost httppost = new HttpPost(
            "https://" + splunkHost + ":" + mgmtPort + "/services/search/jobs");
    List<NameValuePair> params = new ArrayList<>(2);
    params.add(new BasicNameValuePair("output_mode", "json"));
    params.add(new BasicNameValuePair("exec_mode", "blocking")); // splunk won't respond until search is complete
    params.add(new BasicNameValuePair("search", getSearchString()));
    params.add(new BasicNameValuePair("earliest_time", "-5m"));
    params.add(new BasicNameValuePair("latest_time", "now"));
    httppost.setEntity(new UrlEncodedFormEntity(params, "UTF-8"));
    //Execute and get the response.
    HttpResponse postResponse = httpClient.execute(httppost);
    ObjectMapper json = new ObjectMapper();
    String postReply = EntityUtils.toString(postResponse.getEntity(), "utf-8");
    return json.readTree(postReply).path("sid").asText();
  }

  protected void verifyResults(List<Event> sentEvents, Set<String> searchResults) {
    if (sentEvents.size() > 100) {
      throw new RuntimeException(
              "Splunk Search Endpoint can't return more than 100 results at once. "
                      + "And this test is lazy - it will not paginate over many pages of results to verify. "
                      + "So be nice and limit your reconciliation test to 100 events. Or go make this test better with pagination.");
    }
    if (sentEvents.size() != searchResults.size()) {
      Assert.fail(
              "Number of events sent and search results do not match: events_sent=" + sentEvents.
              size() + " search_results=" + searchResults.size());
    }
    for (Event e : sentEvents) {
      String eventText = null;
      ObjectMapper json = new ObjectMapper();
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
      if (!searchResults.remove(eventText.trim())) {
        Assert.fail("Event was not present in search results: " + e.toString());
      } else {
        LOG.trace("Validated event");
      }
    }
    if (searchResults.size() != 0) {
      Assert.fail(
              "Search returned " + searchResults.size() + " more events than were sent.");
    }
  }

  /*
     * Queries Splunk REST API with a job search id to
     * return raw event text of the search results.
   */
  private Set<String> queryJobForResults(HttpClient httpClient, String sid)
          throws IOException {
    Set<String> results = new HashSet<>();
    HttpGet httpget = new HttpGet(
            "https://" + splunkHost + ":" + mgmtPort
            + "/services/search/jobs/" + sid + "/results?output_mode=json");

    HttpResponse getResponse = httpClient.execute(httpget);

    String getReply = EntityUtils.toString(getResponse.getEntity(), "utf-8");
    ObjectMapper mapper = new ObjectMapper();

    JsonNode n = mapper.readTree(getReply);
    if (getReply.toLowerCase().contains("unauthorized")) {
      throw new RuntimeException(getReply);
    }
    for (JsonNode node : n.path("results")) {
      boolean success = results.add(node.path("_raw").asText());
      if (!success) {
        throw new RuntimeException("Events sent to Splunk were not unique.");
      }
    }
    return results;
  }

  private boolean isEventEndpoint() {
    return connection.getSettings().getHecEndpointType().equals(Connection.HecEndpoint.STRUCTURED_EVENTS_ENDPOINT);
  }

}
