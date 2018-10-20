package de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.flinkconnectors;

import de.tuberlin.mcc.geddsprocon.DSPConnectorFactory;
//import de.tuberlin.mcc.geddsprocon.tuple.*;

import de.tuberlin.mcc.geddsprocon.DSPConnectorConfig;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.SocketPool;
import org.apache.flink.api.java.tuple.*;
import org.apache.commons.lang.SerializationUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeInfoParser;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.junit.Assert;
import org.junit.Test;
import org.zeromq.ZMQ;



public class FlinkTests {

    public class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word: sentence.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }
    //
    // NEW PULL BASED APPROACH
    //

    // ======== start test: pull based approach test 1 =======
    @Test
    public void pullBasedApproachSimpleSink1() {
        try {
            ZMQ.Context context = ZMQ.context(1);

            //  Socket to talk to server
            System.out.println("Connecting to hello world server…");

            ZMQ.Socket sender = context.socket(ZMQ.PUSH);
            sender.connect("tcp://localhost:9665");
            String[] testArray = new String[1];
            testArray[0] = "HelloFromFlink a a a a a a a a b b b b b b b b c c c c c c c c";

            for(int i = 0; i < testArray.length; i++) {
                System.out.println("Sending: " + testArray[i]);
                sender.send(SerializationUtils.serialize(testArray[i]), 0);
            }

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            DataStream<String> dataStream = env
                    .addSource((SourceFunction)DSPConnectorFactory.getInstance().createSourceConnector(new DSPConnectorConfig.Builder("localhost", 9665)
                            .withSocketType(SocketPool.SocketType.PULL)
                            .withDSP("flink")
                            .build()), TypeInformation.of(String.class))
                    .flatMap(new Splitter());

            dataStream.addSink((SinkFunction)DSPConnectorFactory.getInstance().createSinkConnector(new DSPConnectorConfig.Builder("localhost", 9656)
                    .withDSP("flink")
                    .withHWM(20)
                    //.withBufferConnectorString("buffer-test")
                    .withTimeout(10000)
                    .build()));

            dataStream.print();

            env.execute("Window WordCount");

            //throw new Exception("test");
        } catch (Exception ex) {
            System.err.println(ex.toString() + ex.getStackTrace());
        }
    }

    @Test
    public void pullBasedApproachComplexSink1() {
        try {
            ZMQ.Context context = ZMQ.context(1);

            //  Socket to talk to server
            System.out.println("Connecting to hello world server…");

            ZMQ.Socket sender = context.socket(ZMQ.PUSH);
            sender.connect("tcp://localhost:9665");
            String[] testArray = new String[5];
            testArray[0] = "HelloFromFlink a a a a a a a a b b b b b b b b a a a a a b b b b";
            testArray[1] = "c c c c a a a b b b";
            testArray[2] = "a a a a d d d d";
            testArray[3] = "e e e e e a a a b";
            testArray[4] = "u u u m m m m m c c c";

            for(int i = 0; i < testArray.length; i++) {
                System.out.println("Sending: " + testArray[i]);
                sender.send(SerializationUtils.serialize(testArray[i]), 0);
            }

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            DataStream<String> dataStream = env
                    .addSource((SourceFunction)DSPConnectorFactory.getInstance().createSourceConnector(new DSPConnectorConfig.Builder("localhost", 9665)
                            .withSocketType(SocketPool.SocketType.PULL)
                            .withDSP("flink")
                            .build()), TypeInformation.of(String.class));

            // Start netcat listener: 'nc -l 9755'
            DataStream<String> dataStream2 = env.socketTextStream("localhost", 9755);


            DataStream<Tuple2<String, Integer>> dataStream3 = dataStream.union(dataStream2).flatMap(new Splitter());

            dataStream3.addSink((SinkFunction)DSPConnectorFactory.getInstance().createSinkConnector(new DSPConnectorConfig.Builder("localhost", 9656)
                    .withDSP("flink")
                    .withHWM(20)
                    .withTimeout(10000)
                    .build()));

            dataStream3.print();

            env.execute("Window WordCount");
        } catch (Exception ex) {
            System.err.println(ex.toString() + ex.getStackTrace());
        }
    }

    @Test
    public void pullBasedApproachPrimarySource1() {
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            DataStream<Tuple2<String, Integer>> dataStream = env
                    .addSource((SourceFunction)DSPConnectorFactory.getInstance().createSourceConnector(new DSPConnectorConfig.Builder()
                            .withDSP("flink")
                            .withRequestAddress("localhost", 9656, DSPConnectorFactory.ConnectorType.PRIMARY)
                            .withRequestAddress("localhost", 9666, DSPConnectorFactory.ConnectorType.PRIMARY)
                            .build()), TypeInfoParser.parse("Tuple2<String,Integer>"))
                    .keyBy("f0")
                    .timeWindow(Time.seconds(5))
                    .sum("f1");

            dataStream.print();

            env.execute("Window WordCount");
        } catch (Exception ex) {
            System.err.println(ex.toString() + ex.getStackTrace().toString());
        }
    }

    @Test
    public void pullBasedApproachSecondarySource1() {
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            DataStream<Tuple2<String, Integer>> dataStream = env
                    .addSource((SourceFunction)DSPConnectorFactory.getInstance().createSourceConnector(new DSPConnectorConfig.Builder()
                            .withDSP("flink")
                            .withRequestAddress("localhost", 9656, DSPConnectorFactory.ConnectorType.SECONDARY)
                            .withRequestAddress("localhost", 9666, DSPConnectorFactory.ConnectorType.SECONDARY)
                            .build()), TypeInfoParser.parse("Tuple2<String,Integer>"))
                    .keyBy("f0")
                    .timeWindow(Time.seconds(5))
                    .sum("f1");

            dataStream.print();

            env.execute("Window WordCount");

            //throw new Exception("Test");
        } catch (Exception ex) {
            System.err.println(ex.toString() + ex.getStackTrace().toString());
        }
    }
    // ======== end test: pull based approach test 1 =======
}
