# Cloud forwarder

Use Cloudfwd to reliably send data to Splunk HTTP Event Collector (HEC) with indexer acknowledgments enabled. Use this README to quickly get started with Cloudfwd. To see how Cloudfwd fits into your system, see [architecture overview](https://github.com/splunk/cloudfwd/wiki/Architecture).

## Table of Contents
- [Cloud forwarder](#cloud-forwarder)
  * [Getting Started](#getting-started)
    + [Prerequisites](#prerequisites)
    + [Installation](#installation)
  * [Usage](#usage)
  * [Wiki](https://github.com/splunk/cloudfwd/wiki)
  * [Connection API Documentation](#connection-api-documentation)
  * [Configurable Property Keys](#configurable-property-keys)
  * [Examples](#examples)
    + [Super Simple Example](#super-simple-example)
    + [Getting data from Amazon Kinesis Streams into Splunk using Cloudfwd](#getting-data-from-amazon-kinesis-streams-into-splunk-using-cloudfwd)
    + [Getting AWS logs into Splunk using Cloudfwd](#getting-aws-logs-into-splunk-using-cloudfwd)
  * [Testing Cloudfwd](#test-cloudfwd)  
  * [Troubleshoot Cloudfwd](#troubleshoot-cloudfwd)
    + [Error exceptions tables](#error-exceptions-tables)
  * [Built With](#built-with)
  * [Version](#version)
  * [License](#license)

## Getting Started

### Prerequisites

1. An external Splunk Enterprise or Splunk Cloud 6.4+ deployment configured with a HTTP Event Collector token to receive data.
2. [Maven](https://maven.apache.org/index.html)
3. [Java 8](http://www.oracle.com/technetwork/java/javase/overview/java8-2100321.html)
4. [HEC Token](http://docs.splunk.com/Documentation/Splunk/latest/Data/UsetheHTTPEventCollector)

### Installation

Make sure that you have the necessary prerequisites before setting up Cloudfwd. 

1.  Get the cloudfwd project folder via ```git clone https://github.com/splunk/cloudfwd.git``` or by downloading and extracting the project zipfile.
2. In the cloudfwd project folder, run ```mvn install```  to build target/cloudfwd-1.0-SNAPSHOT.jar (an "uber jar" containing all dependent classes).<br>
    a. To build cloudfwd and run integration and unit tests together, run ```mvn install -DskipITs=false```
3. Set up HTTP Event Collector and generate a [HEC token](http://docs.splunk.com/Documentation/Splunk/latest/Data/UsetheHTTPEventCollector). Enable indexer acknowledgment for your token by clicking the **Enable indexer acknowledgment** checkbox when creating an Event Collector token.
4. In `cloudfwd.properties`, set your HEC endpoint URL(s) and your generated HEC token. You can also put an ELB destination or multiple host desitnations as the url. 
5. Save your changes.
6. Use the cloudfwd API to send events into HEC.<br> 
	a. See [com.splunk.cloudfwd API javadocs](https://splunk.github.io/cloudfwd/apidocs/index.html?overview-summary.html)

You can now search on your ingested data in your Splunk instance.

### Usage
``` 
// connect to Splunk and send an event
ConnectionCallbacks callbacks = getCallbacks();
Connection connection = Connections.create(callbacks);

Event event = RawEvent.fromText("hello splunk", 0);

connection.send(event);
```

For full usage information, see the [Connection API Documentation](https://splunk.github.io/cloudfwd/apidocs/index.html?overview-summary.html) 

## Wiki 
See [Wiki](https://github.com/splunk/cloudfwd/wiki) for additional documentation, including architecture overview and event formatting best practices.

## Connection API Documentation
[com.splunk.cloudfwd API javadocs](https://splunk.github.io/cloudfwd/apidocs/index.html?overview-summary.html)


## Configurable Property Keys
https://splunk.github.io/cloudfwd/apidocs/constant-values.html

## Examples

### Super Simple Example
[SuperSimpleExample.java](https://github.com/splunk/cloudfwd/blob/master/src/test/java/SuperSimpleExample.java)

### Steps
1. run mvn ```exec com.splunk.cloudfwd.test.SuperSimpleExample``` 

### Getting data from Amazon Kinesis Streams into Splunk using Cloudfwd
This example will use the same configurations set up in the [Amazon Kinesis Stream tutorial](http://docs.aws.amazon.com/streams/latest/dev/learning-kinesis-module-one.html) from the AWS website. We have provided the Java code in the /examples/ folder, but you will need to go through the Kinesis Stream tutorial to setup Kinesis Streams, DynamoDB, and your IAM role. 

### Prerequisites
1. Steps 1 and 2 in the [Amazon Kinesis Stream tutorial](http://docs.aws.amazon.com/streams/latest/dev/learning-kinesis-module-one.html)

### Steps
1. Set up HTTP Event Collector and generate a [HEC token](http://docs.splunk.com/Documentation/Splunk/latest/Data/UsetheHTTPEventCollector). Enable indexer acknowledgment for your token by clicking the Enable indexer acknowledgment checkbox when creating an Event Collector token.
2. In examples > kinesis > resources > cloudfwd.properties, set your HEC endpoint URL(s). You can put an ELB destination or multiple host destinations, separated by commas.
3. In examples > kinesis > resources > cloudfwd.properties, input your generated HEC token.
```
url=https://127.0.0.1:8088
token=80EE7887-EC3E-4D11-95AE-CA9B2DCBB4CB
```
4. You can also input specific host(s), index(es), source(s), or sourcetype(s). 
5. Save your modified cloudfwd.properties file.
6. In your preferred IDE, open the Java project in the /examples/ folder. 
7. Run the StockTradeWriter class with the following arguments: ```<stream_name> <AWS_region_name> <your_profile_name> ```.
8. Run the StockTradeProcessor class with the following arguments: ```<application_name> <stream_name> <AWS_region_name> <profile_name> ```. This might take a few minutes.
9. Go to your Splunk deployment.
10. Switch the time range picker to All time(real-time).
11. Run 'index=*' in Splunk search.

After a minute, stock trade events appear in the Splunk UI.

### Getting AWS logs into Splunk using Cloudfwd
This example uses the Firehose to Splunk Add-on to get AWS logs into your Splunk deployment. This example ships with three types of logfiles: CloudWatch Event logs, sys logs, and VPC Flow logs. 

### Prerequisites 
1.  Splunk Add-on for Amazon Kinesis Firehose
2. A Kinesis stream and an IAM role.

### Steps
1. Set up HTTP Event Collector and generate a [HEC token](http://docs.splunk.com/Documentation/Splunk/latest/Data/UsetheHTTPEventCollector). Enable indexer acknowledgment for your token by clicking the **Enable indexer acknowledgment** checkbox when creating an Event Collector token. 
2. In examples > kinesis > resources > cloudfwd.properties, set your HEC endpoint URL(s). You can put an ELB destination or multiple host destinations, separated by commas.
3. In examples > kinesis > resources > cloudfwd.properties, input your generated HEC token.
```
url=https://127.0.0.1:8088
token=80EE7887-EC3E-4D11-95AE-CA9B2DCBB4CB
```
4. In your preferred IDE, run LogWriter with the following arguments: ```<stream_name> <AWS_region_name> <AWS_profile_name> <name_of_log_file>```<br>
	a. Example: ``` mystream us-west-2 default cloudwatchEventLogs  ```<br>
	b. If you get a dependency incompatability error with version mismatch, set your environment variable to ```AWS_CBOR_DISABLE=true```
5. In your preferred IDE, run LogsProcessor with the following arguments: ```<AWS_app_name> <stream_name> <AWS_region_name> <AWS_profile_name>``` This might take a few minutes.<br>
	a. Example: ```cloudfwd-example-consumer mystream us-west-2 default ```<br>
	b. If you get a dependency incompatability error with version mismatch, set your environment variable to ```AWS_CBOR_DISABLE=true```
6. Open your Splunk environment, and search for your sourcetype in your Splunk environment.

After searching, your log data will appear in the Splunk UI.

## Test Cloudfwd

Cloudfwd comes with a set of unit and integration tests. You can run these with the following commands.
* To run all unit tests, run ```mvn test```. The unit tests use a simulated Splunk server.
* To run integration tests and unit tests together, run ```mvn verify -DskipITs=false```. The integration tests require a Splunk instance to be running, and will send a small amount of data into your Splunk instance using the Cloudfwd client. It will then query the Splunk instance for the data in order to verify it. You must add your instance's username and password to `AbstractReconciliationTest.java`.
* To build cloudfwd and run integration and unit tests together, run ```mvn install -DskipITs=false```


## Troubleshoot Cloudfwd

The following assumes familiarity with Splunk software. 

### If you are not seeing your events in your Splunk environment, make sure that your events were indexed properly in HEC. 

A ```true``` status generally indicates that the events that correspond to that ackID were replicated at the desired replication factor. However, a ```true``` status might also result from events that were dropped in the indexing process due to misconfiguration or another error.  For example, if the event formatting was not consistent with the format specified by ```INDEXED_EXTRACTIONS``` in ```props.conf```, then the event would be dropped and an error logged in ```splunkd.log```. 

If you are sending data using the ```/event``` endpoint and you are not seeing your data in the Splunk software, verify that the settings you are using are correct in  ```INDEXED_EXTRACTIONS``` and ```cloudfwd.properties```.

See [event formatting](https://github.com/splunk/cloudfwd/wiki/Event-Formatting) for information on how to format your events before sending them to the /event or /raw endpoint.

### Error exceptions tables


#### Exceptions passed to failed() callback (asynchronous):
| Exception                       | Description                                                                                                                                                                                                                                      | How to fix                                                                                                                                                                                                                                                                                                                                                                   |
|---------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| HecAckTimeoutException          | This exception gets thrown after waiting for [PropertyKeys.ACK_TIMEOUT_MS](https://splunk.github.io/cloudfwd/apidocs/com/splunk/cloudfwd/PropertyKeys.html#ACK_TIMEOUT_MS) milliseconds for the acknowledgment from your Splunk deployment after sending an event batch.                                                                       | This is not recoverable. When the library fails to send events, the events will not be re-sent. You can backup your events to S3.                                                                                                                                                                                                                                     |
| HecMaxRetriesException          | This exception gets thrown when a channel is dead and the library must re-send event batch. This is the number of attempts to send events after it has reached its maxRetries limit.                                                                                            | This is not recoverable.                                                                                                                                                                                                                                                                                                                                                     |
| HecNonStickySessionException    | This exception gets thrown when a duplicate ackID is received on a given HEC channel. This can indicate failure of a sticky load balancer to provide stickiness.                                                                                                               | This is recoverable and is most likely caused by ELB that does not have sticky sessions enabled. Close the connection and reconfigure your elastic load balancer to have sticky sessions enabled.                                                                                                                                                                            |
| HecServerErrorResponseException | This exception gets thrown when a non-200 response is returned by any Splunk HEC endpoint. It will contain a Splunk server side error code (1-14) as well as the message. For a detailed description of each error code, see the HecServerErrorResponseException class in the API documentation. | It depends. Some error codes are recoverable user configuration errors (e.g. missing token), recoverable user data errors (e.g. incorrect data format passed), or recoverable server errors (e.g. server temporarily busy). While some other error codes are non-recoverable internal library errors that require a fix in the library code or abandoning an existing connection. |
| Misc. Runtime Exceptions        | Runtime exceptions might be caught in the library for logging, and then passed to failed() callback. You can debug this with stacktrace.                                                                                                          | This is not recoverable.                                                                                                                                                                                                                                                                                                                                                     |
#### Runtime exceptions thrown in blocking send() call (synchronous):
You should handle these exceptions in a try-catch block around Connection.sendBatch()
                                                        

| Exception                     | Description                                                                                                                        | How to fix                                                                  |
|-------------------------------|------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------|
| HecConnectionStateException   | This exception gets thrown when there is an illegal state with the connection that indicates caller error, with an enum of types. For a summary of enum constant types, see the [HecConnectionStateException.Type](https://splunk.github.io/cloudfwd/apidocs/com/splunk/cloudfwd/error/HecConnectionStateException.Type.html) class in the API documentation.  | This should be resolved by the caller.                                           |
| HecConnectionTimeoutException | This exception gets thrown when a send() timeout has occurred (exceeded BLOCKING_TIMEOUT_MS).                                      | Restart the connection.                                                     |
| HecIllegalStateException      | This exception gets thrown when Cloudfwd is in an illegal state, with an enum of types. For a summary of enum constant types, see the [HecIllegalStateExcept.Type](https://splunk.github.io/cloudfwd/apidocs/com/splunk/cloudfwd/error/HecIllegalStateException.Type.html) class in the API documentation.                                             | These errors are not recoverable and are purely meant for logging purposes. |
| Runtime Exceptions (Misc)     | This exception gets thrown by libraries used by Cloudfwd (example: Apache HTTP Client).                                            | These errors are not recoverable.                                           |


## Built With

* [Maven](https://maven.apache.org/) - Dependency Management
* [Java](https://www.oracle.com/technetwork/java/javase/overview/java8-2100321.html) - Programming model


## Version

BETA 1

## License

This project is licensed under the Apache 2.0 License - see the [LICENSE.md](LICENSE.md) file for details

