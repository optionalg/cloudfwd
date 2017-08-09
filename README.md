# Cloud forwarder

Use Cloudfwd to capture and ingest data from external sources directly into Splunk indexers. 

## Getting Started

### Prerequisites

1. Splunk 6.4+
2. [Maven](https://maven.apache.org/index.html)
3. [Java 8](http://www.oracle.com/technetwork/java/javase/overview/java8-2100321.html)
4. [HEC Token](http://docs.splunk.com/Documentation/Splunk/6.6.1/Data/UsetheHTTPEventCollector)


### Installation

Make sure that you have the necessary prerequisites before setting up the Cloud forwarder. 

1. Set up HTTP Event Collector and generate a [HEC token](http://docs.splunk.com/Documentation/Splunk/6.6.1/Data/UsetheHTTPEventCollector). Enable indexer acknowledgment for your token by clicking the Enable indexer acknowledgment checkbox when creating an Event Collector token. 
3. In resources > lb.properties, set your destination URL. You can put an ELB destination or multiple host destinations, separated by commas. 
4. In resources > lb. properties, input your generated HEC token.
5. You can also input specific host(s), index(es), source(s), or sourcetype(s). 
6. Save your changes.
7. Run your ....
8. Yay success

You can now search on your ingested data in your Splunk instance.

##Examples

### Getting data from Amazon Kinesis Streams into Splunk using Cloud forwarder
This example will use the same configurations set up in the [Amazon Kinesis Stream tutorial](http://docs.aws.amazon.com/streams/latest/dev/learning-kinesis-module-one.html) from the AWS website. We have provided the Java code in the /examples/ folder, but you will need to go through the Kinesis Stream tutorial to setup Kinesis Streams, DynamoDB, and your IAM role. 

###Prerequisites
1. Steps 1-5 in the [Amazon Kinesis Stream tutorial](http://docs.aws.amazon.com/streams/latest/dev/learning-kinesis-module-one.html)

###Steps
1. Set up HTTP Event Collector and generate a [HEC token](http://docs.splunk.com/Documentation/Splunk/6.6.1/Data/UsetheHTTPEventCollector). Enable indexer acknowledgment for your token by clicking the Enable indexer acknowledgment checkbox when creating an Event Collector token. 
2. In example > resources > lb.properties, set your destination URL. You can put an ELB destination or multiple host destinations, separated by commas. 
3. In example > resources > lb.properties, input your generated HEC token.
4. You can also input specific host(s), index(es), source(s), or sourcetype(s). 
5. Save your modified lb.properties file. 
6. Run the StockTradeWriter class with the following arguments: stream_name AWS_region_name your_profile_name. 
7. Run the StockTradeProcessor class with the following arguments: application_name stream_name AWS_region_name profile_name. This may take a few minutes.
8. Go to the Splunk destination url that you defined in lb.properties
9. Switch the time range picker to All time(real-time)
9. Run 'index=*' in Splunk search. 

After a minute, you should see Splunk output stock trade events.

## Built With

* [Maven](https://maven.apache.org/) - Dependency Management
* [Java](https://www.oracle.com/technetwork/java/javase/overview/java8-2100321.html) - Programming model


## Versioning

Version 1.0 


## License

This project is licensed under the Apache 2.0 License - see the [LICENSE.md](LICENSE.md) file for details

