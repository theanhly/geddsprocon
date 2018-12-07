package de.tuberlin.mcc.geddsprocon.geddsproconevaluation.firstpart.spark;

import de.tuberlin.mcc.geddsprocon.geddsproconcore.DSPConnectorConfig;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.DSPConnectorFactory;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.datastreamprocessorconnectors.SocketPool;
import de.tuberlin.mcc.geddsprocon.geddsproconevaluation.common.ZeroMQDataProvider;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.receiver.Receiver;

public class SparkOutput {

    public static void main(String[] args) throws InterruptedException {
        String host = "192.168.56.102";
        int inputPort = 9665;
        int outPutPort = 9656;

        if(args.length > 2) {
            host = args[0];
            outPutPort = Integer.parseInt(args[1]);
        }

        String file = "/home/theanhly/Schreibtisch/amazon_reviews_us_Video_DVD_v1_00.tsv";
        Thread zeroMQDataProviderThread = new Thread(new ZeroMQDataProvider(host, inputPort, file));
        zeroMQDataProviderThread.start();

        SparkConf sparkConf = new SparkConf()
                .setAppName("JavaCustomReceiver")
                .set("spark.default.parallelism", "1")
                .setMaster("local[2]")
                .set("spark.executor.instances", "1")
                .set("spark.task.cpus", "1");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(5000));


        // Create an input stream with the custom receiver on target ip:port and count the
        // words in input stream of \n delimited text (eg. generated by 'nc')
        JavaReceiverInputDStream<String> lines =
                ssc.receiverStream((Receiver)DSPConnectorFactory.getInstance().createInputOperator(new DSPConnectorConfig.Builder(host, inputPort)
                        .withDSP("spark")
                        .withSocketType(SocketPool.SocketType.PULL)
                        .build()));

        //      Split each line into words
        JavaDStream<String> words = lines.flatMap(new StringSplitter());

        //      Count each word in each batch
        JavaPairDStream<String, Integer> pairs = words.mapToPair(new StringMapper());

        //      Cumulate the sum from each batch
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(
                (Function2<Integer, Integer, Integer>) (i1, i2) -> i1 + i2
        );

        wordCounts.foreachRDD((VoidFunction)DSPConnectorFactory.getInstance().createOutputOperator(new DSPConnectorConfig.Builder(host, outPutPort)
                .withDSP("spark")
                .withHWM(1000)
                .withTimeout(10000)
                .build()));

        ssc.start();
        ssc.awaitTermination();
    }
}
