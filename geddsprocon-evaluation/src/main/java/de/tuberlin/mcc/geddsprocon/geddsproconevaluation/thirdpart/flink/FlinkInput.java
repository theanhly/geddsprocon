package de.tuberlin.mcc.geddsprocon.geddsproconevaluation.thirdpart.flink;

import de.tuberlin.mcc.geddsprocon.geddsproconcore.DSPConnectorConfig;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.DSPConnectorFactory;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeInfoParser;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

public class FlinkInput {

    public static void main(String[] args) {
        try{
            ParameterTool parameters = ParameterTool.fromArgs(args);
            String host = parameters.get("host", "127.0.0.1");
            int inputPort = Integer.parseInt(parameters.get("port", "9656"));
            int bufferSize = Integer.parseInt(parameters.getRequired("buffer"));

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            DataStream<Tuple2<String, Integer>> dataStream = env
                    .addSource((SourceFunction)DSPConnectorFactory.getInstance().createInputOperator(new DSPConnectorConfig.Builder()
                            .withDSP("flink")
                            .withHWM(bufferSize)
                            .withTimeout(15000)
                            //.withBufferConnectorString("recvbuffer")
                            .withRequestAddress(host, inputPort, DSPConnectorFactory.ConnectorType.PRIMARY)
                            .build()), TypeInfoParser.parse("Tuple2<String,Integer>"))
                    //.flatMap(new TupleMapper())
                    .keyBy("f0")
                    .timeWindow(Time.seconds(30))
                    .sum("f1");

            dataStream.print();

            env.execute("FlinkInput");
        } catch(Exception ex) {
            System.err.println(ex.toString());
        }
    }
}
