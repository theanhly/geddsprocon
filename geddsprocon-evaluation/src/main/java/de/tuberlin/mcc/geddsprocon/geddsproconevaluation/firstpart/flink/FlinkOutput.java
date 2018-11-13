package de.tuberlin.mcc.geddsprocon.geddsproconevaluation.firstpart.flink;

import de.tuberlin.mcc.geddsprocon.geddsproconcore.DSPConnectorConfig;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.DSPConnectorFactory;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.datastreamprocessorconnectors.SocketPool;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class FlinkOutput {

    public static void main(String[] args) {
        try{
            String host = "192.168.56.102";
            int inputPort = 9665;
            int outPutPort = 9656;
            String file = "/home/theanhly/Schreibtisch/amazon_reviews_us_Video_DVD_v1_00.tsv";
            Thread zeroMQDataProviderThread = new Thread(new ZeroMQDataProvider(host, inputPort, file));
            zeroMQDataProviderThread.start();

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

            DataStream<String> dataStream = env
                    .addSource((SourceFunction)DSPConnectorFactory.getInstance().createInputOperator(new DSPConnectorConfig.Builder(host, inputPort)
                            .withSocketType(SocketPool.SocketType.PULL)
                            .withDSP("flink")
                            .build()), TypeInformation.of(String.class))
                    .flatMap(new StringSplitter());

            dataStream.addSink((SinkFunction)DSPConnectorFactory.getInstance().createOutputOperator(new DSPConnectorConfig.Builder(host, outPutPort)
                    .withDSP("flink")
                    .withHWM(1200000)
                    .withBufferConnectorString("sendbuffer")
                    .withTimeout(5000)
                    .build()));

            env.execute("Window WordCount");
        } catch(Exception ex) {
            System.err.println(ex.toString());
        }
    }
}
