package de.tuberlin.mcc.geddsprocon.geddsproconevaluation.secondpart.flink;

import de.tuberlin.mcc.geddsprocon.geddsproconcore.DSPConnectorConfig;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.DSPConnectorFactory;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.datastreamprocessorconnectors.SocketPool;
import de.tuberlin.mcc.geddsprocon.geddsproconevaluation.common.ZeroMQDataProvider;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class FlinkOutput {

    public static void main(String[] args) {
        try{
            String host = "127.0.0.1";
            int inputPort = 9665;
            int outPutPort = 9656;

            if(args.length > 2) {
                host = args[0];
                outPutPort = Integer.parseInt(args[1]);
            }

            String file = "/home/theanhly/Schreibtisch/amazon_reviews_us_Video_DVD_v1_00.tsv";
            Thread zeroMQDataProviderThread = new Thread(new ZeroMQDataProvider(host, inputPort, file));
            zeroMQDataProviderThread.start();

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            DataStream<Tuple2<String, Integer>> wordStream = env
                    .addSource((SourceFunction)DSPConnectorFactory.getInstance().createInputOperator(new DSPConnectorConfig.Builder(host, inputPort)
                            .withSocketType(SocketPool.SocketType.PULL)
                            .withDSP("flink")
                            .build()), TypeInformation.of(String.class))
                    .flatMap(new StringSplitter());

            /*DataStream<Tuple2<String, Integer>> dataStream = wordStream
                    .keyBy("f0")
                    .timeWindow(Time.seconds(5))
                    .sum("f1");
                    //.flatMap(new TupleMapper());*/

            /*DataStream<Tuple2<String, Integer>> dataStream2 = dataStream
                    .keyBy("f0")
                    .timeWindow(Time.seconds(20))
                    .sum("f1");*/

            wordStream.addSink((SinkFunction)DSPConnectorFactory.getInstance().createOutputOperator(new DSPConnectorConfig.Builder(host, outPutPort)
                    .withDSP("flink")
                    .withHWM(1000)
                    //.withBufferConnectorString("sendbuffer")
                    .withTimeout(5000)
                    .build()));

            //dataStream.print();

            env.execute("FlinkOutput");
        } catch(Exception ex) {
            System.err.println(ex.toString());
        }
    }
}
