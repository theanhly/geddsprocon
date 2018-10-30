# geddsprocon
**Ge**o-**d**istributed **d**ata **s**tream **pro**cessor **con**nector allows user to connect geo-distributed heterogeneous data stream processors (Flink, Spark). As of now it is only possible to use the data stream processors Apache Spark and Apache Flink.
## How to use
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
