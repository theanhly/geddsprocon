package de.tuberlin.mcc.geddsprocon.geddsproconevaluation.thirdpart.flink;

import de.tuberlin.mcc.geddsprocon.geddsproconcore.DSPConnectorConfig;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.DSPConnectorFactory;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.datastreamprocessorconnectors.SocketPool;
import de.tuberlin.mcc.geddsprocon.geddsproconevaluation.common.AWSS3DataProvider;
import de.tuberlin.mcc.geddsprocon.geddsproconevaluation.common.ZeroMQDataProvider;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

public class CompletePipeline {

    public static void main(String[] args) {
        try{
            String host = "127.0.0.1";
            int port = 9665;
            Thread awsS3DataProvider = new Thread(new AWSS3DataProvider(host, port));
            awsS3DataProvider.start();

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            DataStream<Tuple2<String, Integer>> dataStream = env
                    .addSource((SourceFunction)DSPConnectorFactory.getInstance().createInputOperator(new DSPConnectorConfig.Builder(host, port)
                            .withSocketType(SocketPool.SocketType.PULL)
                            .withDSP("flink")
                            .build()), TypeInformation.of(String.class))
                    .flatMap(new StringSplitter())
                    .keyBy("f0")
                    .timeWindow(Time.seconds(5))
                    .sum("f1");

            DataStream<Tuple2<String, Integer>> dataStream2 = dataStream
                    .keyBy("f0")
                    .timeWindow(Time.seconds(30))
                    .sum("f1");

            dataStream2.print();

            env.execute("CompletePipeline two aggregations");
        } catch(Exception ex) {
            System.err.println(ex.toString());
        }
    }
}
