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

    @Test
    public void sourceTest() {
        try {
            ZMQ.Context context = ZMQ.context(1);

            //  Socket to talk to server
            System.out.println("Connecting to hello world server…");

            ZMQ.Socket sender = context.socket(ZMQ.PUSH);
            sender.connect("tcp://localhost:5555");

            String[] testArray = new String[5];
            testArray[0] = "a a a a a a a a b b b b b b b b a a a a a b b b b";
            testArray[1] = "c c c c a a a b b b";
            testArray[2] = "a a a a d d d d";
            testArray[3] = "e e e e e a a a b";
            testArray[4] = "u u u m m m m m c c c";

            for(int i = 0; i < testArray.length; i++) {
                System.out.println("Sending: " + testArray[i]);
                sender.send(SerializationUtils.serialize(testArray[i]), 0);
            }

            DSPConnectorFactory dspConnectorFactory = new DSPConnectorFactory();
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            DataStream<Tuple2<String, Integer>> dataStream = env
                    //.socketTextStream("localhost", 8080)
                    .addSource((SourceFunction)dspConnectorFactory.createSourceConnector(new DSPConnectorConfig.Builder("localhost", 5555).withDSP("flink").build()), TypeInformation.of(String.class))
                    .flatMap(new Splitter())
                    .keyBy(0)
                    .timeWindow(Time.seconds(5))
                    .sum(1);

            dataStream.print();

            env.execute("Window WordCount");
        } catch (Exception ex) {
            System.err.println(ex.getStackTrace());
        }
    }

    @Test
    public void sinkTest() {
        try {
            ZMQ.Context context = ZMQ.context(1);

            //  Socket to talk to server
            System.out.println("Connecting to hello world server…");

            ZMQ.Socket sender = context.socket(ZMQ.PUSH);
            sender.connect("tcp://localhost:5555");

            String[] testArray = new String[5];
            testArray[0] = "a a a a a a a a b b b b b b b b a a a a a b b b b";
            testArray[1] = "c c c c a a a b b b";
            testArray[2] = "a a a a d d d d";
            testArray[3] = "e e e e e a a a b";
            testArray[4] = "u u u m m m m m c c c";

            for(int i = 0; i < testArray.length; i++) {
                System.out.println("Sending: " + testArray[i]);
                sender.send(SerializationUtils.serialize(testArray[i]), 0);
            }

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            DataStream<Tuple2<String, Integer>> dataStream = env
                    .addSource((SourceFunction)new DSPConnectorFactory().createSourceConnector(new DSPConnectorConfig.Builder("localhost", 5555).withDSP("flink").build()), TypeInformation.of(String.class))
                    .flatMap(new Splitter())
                    .keyBy(0)
                    .timeWindow(Time.seconds(5))
                    .sum(1);

            dataStream.addSink((SinkFunction)new DSPConnectorFactory().createSinkConnector(new DSPConnectorConfig.Builder("localhost", 5556).withDSP("flink").build()));

            dataStream.print();

            env.execute("Window WordCount");
        } catch (Exception ex) {
            System.err.println(ex.getStackTrace());
        }
    }

    // ======== Pipeline test start: test PipelineTest1a and PipelineTest1b are started separately. 1a sends to 1b. ========
    @Test
    public void pipelineTest1a() {
        try {
            ZMQ.Context context = ZMQ.context(1);

            //  Socket to talk to server
            System.out.println("Connecting to hello world server…");

            ZMQ.Socket sender = context.socket(ZMQ.PUSH);
            sender.connect("tcp://localhost:5555");

            String[] testArray = new String[5];
            testArray[0] = "a a a a a a a a b b b b b b b b a a a a a b b b b";
            testArray[1] = "c c c c a a a b b b";
            testArray[2] = "a a a a d d d d";
            testArray[3] = "e e e e e a a a b";
            testArray[4] = "u u u m m m m m c c c";

            for(int i = 0; i < testArray.length; i++) {
                System.out.println("Sending: " + testArray[i]);
                sender.send(SerializationUtils.serialize(testArray[i]), 0);
            }

            DSPConnectorFactory<String> dspConnectorFactory = new DSPConnectorFactory<>();
            DSPConnectorFactory<Tuple2<String, Integer>> dspSinkConnectorFactory = new DSPConnectorFactory<Tuple2<String, Integer>>();
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            DataStream<Tuple2<String, Integer>> dataStream = env
                    .addSource((SourceFunction)dspConnectorFactory.createSourceConnector(new DSPConnectorConfig.Builder("localhost", 5555).withDSP("flink").build()), TypeInformation.of(String.class))
                    .flatMap(new Splitter());

            dataStream.addSink((SinkFunction)dspSinkConnectorFactory.createSinkConnector(new DSPConnectorConfig.Builder("localhost", 5556).withDSP("flink").build()));

            //dataStream.print();

            env.execute("Window WordCount");
        } catch (Exception ex) {
            System.err.println(ex + ": " + ex.getStackTrace());
        }
    }

    @Test
    public void pipelineTest1b() {
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            DataStream<Tuple2<String, Integer>> dataStream = env
                    .addSource((SourceFunction)new DSPConnectorFactory<>().createSourceConnector(new DSPConnectorConfig.Builder("localhost", 5556).withDSP("flink").build()), TypeInfoParser.parse("Tuple2<String,Integer>"))
                    .keyBy("f0")
                    .timeWindow(Time.seconds(5))
                    .sum("f1");

            dataStream.print();

            env.execute("Window WordCount");
        } catch (Exception ex) {
            System.err.println(ex.toString() + ex.getStackTrace().toString());
        }
    }
    // ======== Pipeline test finished ========

    // ======== Receive tuple from Spark test ========
    @Test
    public void pipelineReceiveFromSparkTest1() {
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            DataStream<Tuple2<String, Integer>> dataStream = env
                    .addSource((SourceFunction)new DSPConnectorFactory<>().createSourceConnector(new DSPConnectorConfig.Builder("localhost", 7556).withDSP("flink").build()), TypeInfoParser.parse("Tuple2<String,Integer>"))
                    .keyBy("f0")
                    .timeWindow(Time.seconds(5))
                    .sum("f1");

            dataStream.print();

            env.execute("Window WordCount");
        } catch (Exception ex) {
            System.err.println(ex.toString() + ex.getStackTrace().toString());
        }
    }
    // ======= End test ======

    // ======== Send tuple to Spark test ========
    @Test
    public void pipelineSendToSparkTest1() {
        try {
            ZMQ.Context context = ZMQ.context(1);

            //  Socket to talk to server
            System.out.println("Connecting to hello world server…");

            ZMQ.Socket sender = context.socket(ZMQ.PUSH);
            sender.connect("tcp://localhost:8555");

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

            DSPConnectorFactory<String> dspConnectorFactory = new DSPConnectorFactory<>();
            DSPConnectorFactory<Tuple2<String, Integer>> dspSinkConnectorFactory = new DSPConnectorFactory<Tuple2<String, Integer>>();
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            DataStream<Tuple2<String, Integer>> dataStream = env
                    .addSource((SourceFunction)dspConnectorFactory.createSourceConnector(new DSPConnectorConfig.Builder("localhost", 8555).withDSP("flink").build()), TypeInformation.of(String.class))
                    .flatMap(new Splitter());

            dataStream.addSink((SinkFunction)dspSinkConnectorFactory.createSinkConnector(new DSPConnectorConfig.Builder("localhost", 8556).withDSP("flink").build()));

            //dataStream.print();

            env.execute("Window WordCount");
        } catch (Exception ex) {
            System.err.println(ex.getStackTrace());
        }
    }
    // ======= End test ======

    //
    // NEW PULL BASED APPROACH
    //

    // ======== start test: pull based approach test 1 =======
    @Test
    public void pullBasedApproachSink1() {
        try {
            ZMQ.Context context = ZMQ.context(1);

            //  Socket to talk to server
            System.out.println("Connecting to hello world server…");

            ZMQ.Socket sender = context.socket(ZMQ.PUSH);
            sender.connect("tcp://localhost:9655");
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
                    .addSource((SourceFunction)new DSPConnectorFactory().createSourceConnector(new DSPConnectorConfig.Builder("localhost", 9655)
                            .withSocketType(SocketPool.SocketType.PULL)
                            .withDSP("flink")
                            .build()), TypeInformation.of(String.class));

            // Start netcat listener: 'nc -l 9755'
            DataStream<String> dataStream2 = env.socketTextStream("localhost", 9755);


            DataStream<Tuple2<String, Integer>> dataStream3 = dataStream.union(dataStream2).flatMap(new Splitter());

            dataStream3.addSink((SinkFunction)new DSPConnectorFactory().createSinkConnector(new DSPConnectorConfig.Builder("localhost", 9656)
                    .withDSP("flink")
                    .withTimeout(10000)
                    .build()));

            dataStream3.print();

            env.execute("Window WordCount");
        } catch (Exception ex) {
            System.err.println(ex.toString() + ex.getStackTrace());
        }
    }

    @Test
    public void pullBasedApproachSource1() {
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            DataStream<Tuple2<String, Integer>> dataStream = env
                    .addSource((SourceFunction)new DSPConnectorFactory<>().createSourceConnector(new DSPConnectorConfig.Builder("localhost", 9656)
                            .withDSP("flink")
                            .withConnectorType(DSPConnectorFactory.ConnectorType.PRIMARY)
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
    // ======== end test: pull based approach test 1 =======
}
