# Cloud forwarder

Use Cloudfwd to reliably send data to Splunk Http Event Collector (HEC) with indexer acknowledgments enabled. 

## Getting Started

### Prerequisites

1. An external Splunk Enterprise or Splunk Cloud 6.x deployment configured with a HTTP Event Collector token to receive data.
2. [Maven](https://maven.apache.org/index.html)
3. [Java 8](http://www.oracle.com/technetwork/java/javase/overview/java8-2100321.html)
4. [HEC Token](http://docs.splunk.com/Documentation/Splunk/6.6.1/Data/UsetheHTTPEventCollector)
5. Link_to_Required_Add-On


### Installation

Make sure that you have the necessary prerequisites before setting up Cloudfwd. 

1. Set up HTTP Event Collector and generate a [HEC token](http://docs.splunk.com/Documentation/Splunk/6.6.1/Data/UsetheHTTPEventCollector). Enable indexer acknowledgment for your token by clicking the Enable indexer acknowledgment checkbox when creating an Event Collector token. 
3. In resources > lb.properties, set your HEC endpoint URLs. You can put an ELB destination or multiple host destinations, separated by commas. 
4. In resources > lb. properties, input your generated HEC token.
```
url=https://127.0.0.1:8088, https://localhost:8088
token=80EE7887-EC3E-4D11-95AE-CA9B2DCBB4CB
```
5. You can also input specific host(s), index(es), source(s), or sourcetype(s). 
6. Save your changes.
7. Use the Cloudfwd API to send events into HEC. 
	a. See [com.splunk.cloudfwd API javadocs](https://splunk.github.io/cloudfwd/apidocs/index.html?overview-summary.html)

You can now search on your ingested data in your Splunk instance.

## Connection API Documentation
[com.splunk.cloudfwd API javadocs](https://splunk.github.io/cloudfwd/apidocs/index.html?overview-summary.html)

## Configurable Property Keys
https://splunk.github.io/cloudfwd/apidocs/constant-values.html

## Examples

### Super Simple Example
[SuperSimpleExample.java](https://github.com/splunk/cloudfwd/blob/master/src/test/java/SuperSimpleExample.java)

### Getting data from Amazon Kinesis Streams into Splunk using Cloudfwd
This example will use the same configurations set up in the [Amazon Kinesis Stream tutorial](http://docs.aws.amazon.com/streams/latest/dev/learning-kinesis-module-one.html) from the AWS website. We have provided the Java code in the /examples/ folder, but you will need to go through the Kinesis Stream tutorial to setup Kinesis Streams, DynamoDB, and your IAM role. 

### Prerequisites
1. Steps 1 and 2 in the [Amazon Kinesis Stream tutorial](http://docs.aws.amazon.com/streams/latest/dev/learning-kinesis-module-one.html)

### Steps
1. Set up HTTP Event Collector and generate a [HEC token](http://docs.splunk.com/Documentation/Splunk/6.6.1/Data/UsetheHTTPEventCollector). Enable indexer acknowledgment for your token by clicking the Enable indexer acknowledgment checkbox when creating an Event Collector token. 
2. In examples > resources > lb.properties, set your HEC endpoint URL(s). You can put an ELB destination or multiple host destinations, separated by commas. 
3. In examples > resources > lb.properties, input your generated HEC token.
```
url=https://127.0.0.1:8088
token=80EE7887-EC3E-4D11-95AE-CA9B2DCBB4CB
```
4. You can also input specific host(s), index(es), source(s), or sourcetype(s). 
5. Save your modified lb.properties file. 
6. In your preferred IDE, open the Java project in the /examples/ folder. 
7. Run the StockTradeWriter class with the following arguments: ```<stream_name> <AWS_region_name> <your_profile_name> ```.
8. Run the StockTradeProcessor class with the following arguments: ```<application_name> <stream_name> <AWS_region_name> <profile_name> ```. This may take a few minutes.
9. Go to your Splunk deployment.
10. Switch the time range picker to All time(real-time).
11. Run 'index=*' in Splunk search. 

After a minute, you should see stock trade events in the Splunk UI.

## Troubleshoot Cloudfwd

The following assumes familiarity with Splunk software. 

### 1. Make sure that your events were indexed properly in HEC. 

A ```true``` status generally indicates that the event(s) that correspond to that ackID were replicated at the desired replication factor. However, a ```true``` status may also result from event(s) that were dropped in the indexing process due to misconfiguration or another error.  For example, if the event formatting was not consistent with the format specified by ```INDEXED_EXTRACTIONS``` (in ```props.conf```), then the event would be dropped and an error logged in ```splunkd.log```. 

If you are sending data using the ```/event``` endpoint and you are not seeing your data in the Splunk software, verify that the settings you are using are correct in  ```INDEXED_EXTRACTIONS``` and ```lb.properties```. 

## Built With

* [Maven](https://maven.apache.org/) - Dependency Management
* [Java](https://www.oracle.com/technetwork/java/javase/overview/java8-2100321.html) - Programming model


## Version

BETA 1

## License

This project is licensed under the Apache 2.0 License - see the [LICENSE.md](LICENSE.md) file for details
