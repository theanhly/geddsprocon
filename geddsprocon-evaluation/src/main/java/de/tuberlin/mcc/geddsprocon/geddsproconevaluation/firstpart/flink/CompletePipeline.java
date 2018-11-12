package de.tuberlin.mcc.geddsprocon.geddsproconevaluation.firstpart.flink;

import de.tuberlin.mcc.geddsprocon.geddsproconcore.DSPConnectorConfig;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.DSPConnectorFactory;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.datastreamprocessorconnectors.SocketPool;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

public class CompletePipeline {

    public static void main(String[] args) {
        try{
            String host = "localhost";
            int port = 9665;
            String file = "/home/theanhly/Schreibtisch/amazon_reviews_us_Video_DVD_v1_00.tsv";
            Thread zeroMQDataProviderThread = new Thread(new ZeroMQDataProvider(host, port, file));
            zeroMQDataProviderThread.start();

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            DataStream<String> dataStream = env
                    .addSource((SourceFunction)DSPConnectorFactory.getInstance().createInputOperator(new DSPConnectorConfig.Builder(host, port)
                            .withSocketType(SocketPool.SocketType.PULL)
                            .withDSP("flink")
                            .build()), TypeInformation.of(String.class))
                    .flatMap(new StringSplitter())
                    .keyBy("f0")
                    .timeWindow(Time.seconds(5))
                    .sum("f1");;

            dataStream.print();

            env.execute("Window WordCount");
        } catch(Exception ex) {
            System.err.println(ex.toString());
        }
    }
}
