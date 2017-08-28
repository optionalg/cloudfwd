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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.splunk.cloudfwd.Connection;
import com.splunk.cloudfwd.Event;
import com.splunk.cloudfwd.http.HttpClientFactory;
import com.splunk.cloudfwd.util.PropertiesFileHelper;

import java.io.IOException;
import java.util.*;

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
import org.junit.Test;
import java.util.concurrent.TimeoutException;

import org.apache.http.*;


/**
 * NOTES:
 * - Each event sent in a test MUST have unique content for the test to pass,
 *      (add a sequence number in the event text).
 * - Configure the sourcetype in Splunk to break on every newline.
 *
 * @author eprokop
 */
public class ReconciliationTest extends AbstractConnectionTest {

    /* ************ CONFIGURABLE ************ */
    // change these settings based the Splunk instance you're sending data to
    protected int numToSend = 10;
    private String splunkHost = "localhost";
    private String mgmtPort = "8089"; // management port on the Splunk search head
    private String index = "dummydata"; // where the data is indexed
    private String user = "admin"; // a Splunk user that has permissions to search in <index>
    private String password = "a";
    /* ************ /CONFIGURABLE ************ */

    public ReconciliationTest() {
    }

    @Override
    protected void configureConnection(Connection connection) {
        connection.setCharBufferSize(1024*32); //32k batching batching, roughly
    }

    @Test
    public void sendTextToRawEndpoint() throws InterruptedException, TimeoutException {
        connection.setHecEndpointType(Connection.HecEndpoint.RAW_EVENTS_ENDPOINT);
        super.eventType = EventType.TEXT;
        super.sendEvents();
        Set<String> searchResults = getEventsFromSplunk();
        verifyResults(getSentEvents(), searchResults);
    }

    @Test
    public void sendJsonToRawEndpoint() throws InterruptedException, TimeoutException {
        connection.setHecEndpointType(Connection.HecEndpoint.RAW_EVENTS_ENDPOINT);
        super.eventType = EventType.JSON;
        super.sendEvents();
        Set<String> searchResults = getEventsFromSplunk();
        verifyResults(getSentEvents(), searchResults);
    }

    @Test
    public void sendTextToEventsEndpoint() throws InterruptedException, TimeoutException {
        connection.setHecEndpointType(Connection.HecEndpoint.STRUCTURED_EVENTS_ENDPOINT);
        super.eventType = EventType.TEXT;
        super.sendEvents();
        Set<String> searchResults = getEventsFromSplunk();
        verifyResults(getSentEvents(), searchResults);
    }

    @Test
    public void sendJsonToEventsEndpoint() throws InterruptedException, TimeoutException {
        connection.setHecEndpointType(Connection.HecEndpoint.STRUCTURED_EVENTS_ENDPOINT);
        super.eventType = EventType.JSON;
        super.sendEvents();
        Set<String> searchResults = getEventsFromSplunk();
        verifyResults(getSentEvents(), searchResults);
    }

    @Override
    protected Properties getProps() {
        Properties props = new Properties();
        props.put(PropertiesFileHelper.MOCK_HTTP_KEY, "false");
        return props;
    }

    @Override
    protected int getNumEventsToSend() {
        return numToSend;
    }

    @Override
    protected boolean shouldCacheEvents() {
        return true;
    }

    private Set<String> getEventsFromSplunk() {
        Set<String> results = new HashSet<>();
        try {
            // credentials
            CredentialsProvider credsProvider = new BasicCredentialsProvider();
            credsProvider.setCredentials(
                    new AuthScope(splunkHost, new Integer(mgmtPort)),
                    new UsernamePasswordCredentials(user, password));

            // create synchronous http client that ignores SSL
            HttpClient httpClient = HttpClientBuilder.create()
                    .setDefaultCredentialsProvider(credsProvider)
                    .setHostnameVerifier(SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER)
                    .setSSLContext(HttpClientFactory.build_ssl_context_allow_all())
                    .build();

            String sid = createSearchJob(httpClient);
            results = queryJobForResults(httpClient, sid);
        } catch (Exception e) {
            Assert.fail("Problem getting search results from Splunk: " + e.getMessage());
        }
        return results;
    }

    private String getSearchString() {
        return new StringBuilder()
            .append("search index=")
            .append(index)
            .append(" | extract kvdelim=\"=\" | search ")
            .append(testMethodGUIDKey)
            .append("=")
            .append(testMethodGUID)
            .toString();
    }

    /*
     * Hits Splunk REST API to create a new search job and
     * returns the search id of the job.
     */
    private String createSearchJob(HttpClient httpClient) throws IOException {
        // POST to create a new search job
        HttpPost httppost = new HttpPost("https://" + splunkHost + ":" + mgmtPort + "/services/search/jobs");
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

    /*
     * Queries Splunk REST API with a job search id to
     * return raw event text of the search results.
     */
    private Set<String> queryJobForResults(HttpClient httpClient, String sid) throws IOException {
        Set<String> results = new HashSet<>();
        HttpGet httpget = new HttpGet(
                "https://" + splunkHost + ":" + mgmtPort +
                        "/services/search/jobs/" + sid + "/results?output_mode=json");

        HttpResponse getResponse = httpClient.execute(httpget);

        String getReply = EntityUtils.toString(getResponse.getEntity(), "utf-8");
        ObjectMapper json = new ObjectMapper();
        JsonNode resultsNode = json.readTree(getReply).path("results");
        for (JsonNode node : resultsNode) {
            boolean success = results.add(node.path("_raw").asText());
            if (!success) {
                throw new RuntimeException("Events sent to Splunk were not unique.");
            }
        }
        return results;
    }

    private void verifyResults(List<Event> sentEvents, Set<String> searchResults) {
        if (sentEvents.size() != searchResults.size()) {
            Assert.fail("Number of events sent and search results do not match: events_sent=" +
                sentEvents.size() + " search_results=" + searchResults.size());
        }

        for (Event e : sentEvents) {
            String eventText = e.toString();
            try {
                // extract the event text from the "event" key
                ObjectMapper json = new ObjectMapper();
                if (isJsonToEvent()) {
                    eventText = json.readTree(eventText).path("event").toString();
                } else if (isTextToEvent()) {
                    eventText = json.readTree(eventText).path("event").asText();
                }
            } catch (IOException e1) {
                Assert.fail("Could not parse 'event' key from JSON object: " + e1.getMessage());
            }
            if (!searchResults.remove(eventText.trim())) {
                Assert.fail("Event was not present in search results: " + e.toString());
            }
        }

        if (searchResults.size() != 0) {
            Assert.fail("Search returned " + searchResults.size() + " more events than were sent.");
        }
    }



    private boolean isJsonToEvent() {
        return connection.getHecEndpointType().equals(Connection.HecEndpoint.STRUCTURED_EVENTS_ENDPOINT)
                && eventType.equals(EventType.JSON);
    }

    private boolean isTextToEvent() {
        return connection.getHecEndpointType().equals(Connection.HecEndpoint.STRUCTURED_EVENTS_ENDPOINT)
                && eventType.equals(EventType.TEXT);
    }
}
