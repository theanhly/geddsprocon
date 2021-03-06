/*
 * Copyright 2019 The-Anh Ly
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.tuberlin.mcc.geddsprocon.geddsproconevaluation.firstpart.spark;

import de.tuberlin.mcc.geddsprocon.geddsproconcore.DSPConnectorConfig;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.DSPConnectorFactory;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.SocketPool;
import de.tuberlin.mcc.geddsprocon.geddsproconevaluation.common.ZeroMQDataProvider;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.receiver.Receiver;

public class CompletePipeline {
    public static void main(String[] args) throws InterruptedException {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        String host = parameters.get("host", "0.0.0.0");
        int port = Integer.parseInt(parameters.get("port", "9665"));

        String file = "/home/hadoop/thesis-evaluation/amazon_reviews_us_Video_DVD_v1_00.tsv";
        Thread zeroMQDataProviderThread = new Thread(new ZeroMQDataProvider(host, port, file));
        zeroMQDataProviderThread.start();

        SparkConf sparkConf = new SparkConf()
                .setAppName("Spark_complete_word_count_pipeline");
                //.setMaster("local[*]") // use local as master if test is run locally
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(5000));


        // Create an input stream with the custom receiver on target ip:port and count the
        // words in input stream of \n delimited text (eg. generated by 'nc')
        JavaReceiverInputDStream<String> lines =
                ssc.receiverStream((Receiver)DSPConnectorFactory.getInstance().createInputOperator(new DSPConnectorConfig.Builder(host, port)
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

        wordCounts.print();
        ssc.start();
        ssc.awaitTermination();
    }
}
