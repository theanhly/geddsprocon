package de.tuberlin.mcc.geddsprocon.geddsproconevaluation.firstpart.flink;

import de.tuberlin.mcc.geddsprocon.geddsproconcore.DSPConnectorConfig;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.DSPConnectorFactory;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.datastreamprocessorconnectors.SocketPool;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeInfoParser;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

public class FlinkInput {

    public static void main(String[] args) {
        try{
            String host = "192.168.56.102";
            int inputPort = 9656;

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            DataStream<Tuple2<String, Integer>> dataStream = env
                    .addSource((SourceFunction)DSPConnectorFactory.getInstance().createInputOperator(new DSPConnectorConfig.Builder()
                            .withDSP("flink")
                            .withHWM(300000)
                            .withBufferConnectorString("recvbuffer")
                            .withRequestAddress(host, inputPort, DSPConnectorFactory.ConnectorType.PRIMARY)
                            .build()), TypeInfoParser.parse("Tuple2<String,Integer>"))
                    //.flatMap(new TupleMapper())
                    .keyBy("f0")
                    .timeWindow(Time.seconds(5))
                    .sum("f1");

            dataStream.print();

            env.execute("Window WordCount");
        } catch(Exception ex) {
            System.err.println(ex.toString());
        }
    }
}
