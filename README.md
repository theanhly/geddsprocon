# geddsprocon
**Ge**o-**d**istributed **d**ata **s**tream **pro**cessor **con**nector allows user to connect geo-distributed heterogeneous data stream processors (Flink, Spark). As of now it is only possible to use the data stream processors Apache Spark and Apache Flink.
## How to use geddsprocon ... 
1. Clone this project
2. Use Maven to install the project into your local repository: ```mvn clean install```
3. Add the project into your Spark/Flink pom:
```xml
<dependency>
	<groupId>de.tuberlin.mcc.geddsprocon</groupId>
	<artifactId>geddsprocon</artifactId>
	<version>1.0-SNAPSHOT</version>
</dependency>
```
### ... with Apache Flink
Apache Flink allows creation of custom sources and sinks. We will use that concept to put our input and output operators into such a custom source and sink.
#### Example: Input operator as a custom source
```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

DataStream<Tuple2<String, Integer>> dataStream = env.addSource(
	(SourceFunction)DSPConnectorFactory
		.getInstance()
		.createSourceConnector(
			new DSPConnectorConfig.Builder()
	    			.withDSP("flink")
	    			.withRequestAddress("localhost", 9656, DSPConnectorFactory.ConnectorType.PRIMARY)
	    			.withRequestAddress("localhost", 9666, DSPConnectorFactory.ConnectorType.PRIMARY)
	    			.build())
			, TypeInfoParser.parse("Tuple2<String,Integer>"))
```
#### Example: Output operator as a custom sink
```java
dataStream.addSink(
	(SinkFunction)DSPConnectorFactory
		.getInstance()
		.createSinkConnector(
			new DSPConnectorConfig.Builder("localhost", 9656)
                    		.withDSP("flink")
                    		.withHWM(20)
                    		.withTimeout(10000)
                    		.build()));
```
### ... with Apache Spark
Apache Spark allows creation of custom receivers. For emitting data to a third party we will use Spark's ```foreachRDD(...)``` method. That allows us to insert our output operator as a ```VoidFunction```
#### Example: Input operator as a custom receiver
```java
SparkConf sparkConf = new SparkConf().setAppName("JavaCustomReceiver").setMaster("local[*]");
JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(5000));

JavaReceiverInputDStream<Tuple2<String, Integer>> tuples = ssc.receiverStream(
	(Receiver)DSPConnectorFactory
		.getInstance()
		.createSourceConnector(
			new DSPConnectorConfig.Builder()
                    		.withDSP("spark")
                    		.withRequestAddress("localhost", 9656, DSPConnectorFactory.ConnectorType.PRIMARY)
                    		.withRequestAddress("localhost", 9666, DSPConnectorFactory.ConnectorType.PRIMARY)
                    		.build()));
```
#### Example: Output operator as a VoidFunction
```java
pairs.foreachRDD(
	(VoidFunction)DSPConnectorFactory
		.getInstance()
		.createSinkConnector(
			new DSPConnectorConfig.Builder("localhost", 9656)
			    .withDSP("spark")
			    .withHWM(20)
			    .withTimeout(10000)
			    .build()));
```
