# geddsprocon
**Ge**o-**d**istributed **d**ata **s**tream **pro**cessor **con**nector allows user to connect geo-distributed heterogeneous data stream processors (Flink, Spark). As of now it is only possible to use the data stream processors Apache Spark and Apache Flink.
## How to use geddsprocon ... 
1. Clone this project
2. Use Maven to install the project into your local repository: ```mvn clean install```
3. Add the project dependency to your Spark/Flink Maven project:
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
			new DSPConnectorConfig
				.Builder()
	    			.withDSP("flink")
				.withBufferConnectorString("buffer-connection-string")
	    			.withRequestAddress("31.161.244.160"
					, 9656
					, DSPConnectorFactory.ConnectorType.PRIMARY)
	    			.withRequestAddress("228.206.65.0"
					, 9666
					, DSPConnectorFactory.ConnectorType.PRIMARY)
	    			.build())
			, TypeInfoParser.parse("Tuple2<String,Integer>"))
```
#### Example: Output operator as a custom sink
```java
dataStream.addSink(
	(SinkFunction)DSPConnectorFactory
		.getInstance()
		.createSinkConnector(
			new DSPConnectorConfig
				.Builder("localhost", 9656)
                    		.withDSP("flink")
				.withBufferConnectorString("buffer-connection-string")
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
			new DSPConnectorConfig
				.Builder()
                    		.withDSP("spark")
				.withBufferConnectorString("buffer-connection-string")
                    		.withRequestAddress("43.89.42.250"
					, 9656
					, DSPConnectorFactory.ConnectorType.PRIMARY)
                    		.withRequestAddress("152.182.83.148"
					, 9666
					, DSPConnectorFactory.ConnectorType.PRIMARY)
                    		.build()));
```
#### Example: Output operator as a VoidFunction
```java
tuples.foreachRDD(
	(VoidFunction)DSPConnectorFactory
		.getInstance()
		.createSinkConnector(
			new DSPConnectorConfig
				.Builder("localhost", 9656)
				.withBufferConnectorString("buffer-connection-string")
				.withDSP("spark")
				.withHWM(20)
				.withTimeout(10000)
				.build()));
```
## DSPConnectorFactory
The `DSPConnectorFactory`is the main class which creates the input/output operators. 

Function | Return | Description
---- | --- | ----
`createSourceConnector(DSPConnectorConfig config)` | `IDSPSourceConnector` | This creates the input operator. A cast to the resepective DSP (e.g., `SourceFunction` for Flink, `Receiver` for Spark) is still needed.
`createSinkConnector(DSPConnectorConfig config)` | `IDSPSinkConnector` | This creates the output operator. A cast to the resepective DSP (e.g., `SinkFunction` for Flink, `VoidFunction` for Spark) is still needed. 

Both functions need a `DSPConnectorConfig` to set up the operators.

## DSPConnectorConfig.Builder
The ```DSPConnectorConfig.Builder``` is used to build a `DSPConnectorConfig` which sets up the output/input operators. Below we will describe what the user can set and in which context the settings are valid.

The builder has one specific constructor which is valid for the output operator

Constructor | Description
--- | ---
`public Builder(String adrress, int port)` | Determines IP address and port at which the DSP router is reachable for a DSP requester. The `address`should therefore always be `"localhost"`.

The following table describes the settings which are available.

 Method | Values |  Setting Description 
 ----- | --- | -------- 
`withDSP(String dspString)` | `"flink"`, `"spark"` | Determine which DSP context the input or output operators are used in. |
`withHWM(int hwm)` | `[1, 2147483647]` | The maximum amount of messages the message-buffer should hold. Its default value is `1000`.
 `withoutTransformation()` | - | If the connected DSPs are homogeneous, transformation to intermediate tuples are unnecessary. Using this method turns transformation to and from intermediate transformation off. Transformation is `true` by default.|
`withTimeout(int timeout)` | `[1, 2147483647]` | Only valid for input operators. Input operators request timeout in ms. After `timeout` ms the DSP requester sends another request.
`withBufferConnectorString(String connectorString)` | String | This sets the connection string to the message-buffer process. If the DSP application restarts due to failure the connection string determines the message-buffer. It is highly recommended to set a buffer string in case of failure. If this setting is omitted an internal unused connection string is generated.
`withRequestAddress(String adrress, int port, String connectorType)` | `Output operator IP string`, `Output operator port`, `["PRIMARY", "SECONDARY"]`  | Only valid for input operators. Determines the IP of the output operator. The `connectorType`determines if it is a primary connection or a secondary connection.
