package de.tuberlin.mcc.geddsprocon.geddsproconevaluation.firstpart.spark;

import de.tuberlin.mcc.geddsprocon.geddsproconcore.DSPConnectorConfig;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.DSPConnectorFactory;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.receiver.Receiver;
import scala.Tuple2;

public class SparkInput {

    public static void main(String[] args) throws InterruptedException {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        String host = parameters.get("host", "127.0.0.1");
        int inputPort = Integer.parseInt(parameters.get("port", "9656"));
        int bufferSize = Integer.parseInt(parameters.getRequired("buffer"));

        SparkConf sparkConf = new SparkConf()
                //.set("spark.task.cpus", "1")
                //.set("spark.default.parallelism", "1")
                //.set("spark.streaming.backpressure.enabled", "true")
                //.set("spark.executor.memory","2g")
                //.setMaster("local[3]")
                .setAppName("JavaCustomReceiver");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(5000));

        // Create an input stream with the custom receiver on target ip:port and count the
        // words in input stream of \n delimited text (eg. generated by 'nc')
        JavaReceiverInputDStream<Tuple2<String, Integer>> tuples = ssc.receiverStream((Receiver)DSPConnectorFactory.getInstance().createInputOperator(new DSPConnectorConfig.Builder()
                .withDSP("spark")
                .withRequestAddress(host, inputPort, DSPConnectorFactory.ConnectorType.SECONDARY)
                .withTimeout(1000)
                .withHWM(bufferSize)
                .build()));

        //      Count each word in each batch
        JavaPairDStream<String, Integer> pairs = tuples.mapToPair(
                (PairFunction<Tuple2<String, Integer>, String, Integer>) s -> new Tuple2<>(s._1, s._2)
        );


        //      Cumulate the sum from each batch
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(
                (Function2<Integer, Integer, Integer>) (i1, i2) -> i1 + i2
        );

        wordCounts.print();
        ssc.start();
        ssc.awaitTermination();
    }
}
