package de.tuberlin.mcc.geddsprocon.geddsproconcore.datastreamprocessorconnectors.flinkconnectors;

import de.tuberlin.mcc.geddsprocon.geddsproconcore.DSPConnectorFactory;
//import de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.*;

import de.tuberlin.mcc.geddsprocon.geddsproconcore.DSPConnectorConfig;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.common.SerializationTool;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.SocketPool;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeInfoParser;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.junit.Test;
import org.zeromq.ZMQ;

/**
 * (SINK) --sends_to--> (SOURCE)
 * If the test is a sink it takes test data and sends it to source.
 * Start a sink first and start a source after. The sink should send the test data to the source where it should be printed out.
 */

public class FlinkTests {

    public class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        private boolean throwExc;
        public Splitter()
        {
            this(false);
        }

        public Splitter(boolean throwExc)
        {
            this.throwExc = throwExc;
        }

        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word: sentence.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));

                if(this.throwExc && word.equals("throw_exception")) {
                    Thread.sleep(10000);
                    System.err.println("Exception provoked");
                    throw new Exception("Exception provoked!");
                }
            }
        }
    }

    public class TupleMapper implements FlatMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {
        private long exCounter = 0;
        private boolean throwExc;
        public TupleMapper() {
            this(false);
        }

        public TupleMapper(boolean throwExec) {
            this.throwExc = throwExec;
        }

        @Override
        public void flatMap(Tuple2<String, Integer> inputTuple, Collector<Tuple2<String, Integer>> out) throws Exception {
            if(this.throwExc && this.exCounter < 1 && inputTuple.f0.equals("throw_exception")) {
                //Thread.sleep(10000);
                System.err.println("Exception provoked");
                this.exCounter++;
                throw new Exception("Exception provoked");
            } else {
                out.collect(inputTuple);
            }
        }
    }
    //
    // NEW PULL BASED APPROACH
    //

    // ======== start test: pull based approach test 1 =======
    /**
     * (SINK)
     */
    @Test
    public void pullBasedApproachSimpleSink1() {
        try {
            ZMQ.Context context = ZMQ.context(1);

            //  Socket to talk to server
            System.out.println("Connecting to hello world server…");

            ZMQ.Socket sender = context.socket(ZMQ.PUSH);
            sender.connect("tcp://localhost:9665");
            String[] testArray = new String[1];
            testArray[0] = "HelloFromFlink a b c d e f g h i j k l m n o p q r s t u v w x y z";

            for(int i = 0; i < testArray.length; i++) {
                System.out.println("Sending: " + testArray[i]);
                sender.send(SerializationTool.serialize(testArray[i]), 0);
            }

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            DataStream<String> dataStream = env
                    .addSource((SourceFunction)DSPConnectorFactory.getInstance().createInputOperator(new DSPConnectorConfig.Builder("localhost", 9665)
                            .withSocketType(SocketPool.SocketType.PULL)
                            .withDSP("flink")
                            .build()), TypeInformation.of(String.class))
                    .flatMap(new Splitter());

            dataStream.addSink((SinkFunction)DSPConnectorFactory.getInstance().createOutputOperator(new DSPConnectorConfig.Builder("localhost", 9656)
                    .withDSP("flink")
                    .withHWM(20)
                    //.withBufferConnectorString("sendbuffer")
                    .withTimeout(10000)
                    .build()));

            dataStream.print();

            env.execute("Window WordCount");

            //throw new Exception("test");
        } catch (Exception ex) {
            System.err.println(ex.toString() + ex.getStackTrace());
        }
    }

    /**
     * (SINK)
     * BEFORE STARTING THE TEST: complex sink needs a netcat listener at port 9755.
     * 1. start a terminal
     * 2. nc -l 9755
     * 3. start this test
     */
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
                sender.send(SerializationTool.serialize(testArray[i]), 0);
            }

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            DataStream<String> dataStream = env
                    .addSource((SourceFunction)DSPConnectorFactory.getInstance().createInputOperator(new DSPConnectorConfig.Builder("localhost", 9665)
                            .withSocketType(SocketPool.SocketType.PULL)
                            .withDSP("flink")
                            .build()), TypeInformation.of(String.class));

            // Start netcat listener: 'nc -l 9755'
            DataStream<String> dataStream2 = env.socketTextStream("localhost", 9755);


            DataStream<Tuple2<String, Integer>> dataStream3 = dataStream.union(dataStream2).flatMap(new Splitter());

            dataStream3.addSink((SinkFunction)DSPConnectorFactory.getInstance().createOutputOperator(new DSPConnectorConfig.Builder("localhost", 9656)
                    .withDSP("flink")
                    .withHWM(20)
                    //.withBufferConnectorString("sendbuffer")
                    .withTimeout(10000)
                    .build()));

            dataStream3.print();

            env.execute("Window WordCount");
        } catch (Exception ex) {
            System.err.println(ex.toString() + ex.getStackTrace());
        }
    }

    /**
     * (SOURCE)
     */
    @Test
    public void pullBasedApproachPrimarySource1() {
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            DataStream<Tuple2<String, Integer>> dataStream = env
                    .addSource((SourceFunction)DSPConnectorFactory.getInstance().createInputOperator(new DSPConnectorConfig.Builder()
                            .withDSP("flink")
                            //.withBufferConnectorString("recvbuffer")
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

    /**
     * (SOURCE)
     */
    @Test
    public void pullBasedApproachSecondarySource1() {
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            DataStream<Tuple2<String, Integer>> dataStream = env
                    .addSource((SourceFunction)DSPConnectorFactory.getInstance().createInputOperator(new DSPConnectorConfig.Builder()
                            .withDSP("flink")
                            .withBufferConnectorString("recvbuffer")
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


    // ======== Exception test ========
    /**
     * (SINK)
     */
    @Test
    public void exceptionSimpleSink1() {
        try {
            ZMQ.Context context = ZMQ.context(1);

            //  Socket to talk to server
            System.out.println("Connecting to hello world server…");

            ZMQ.Socket sender = context.socket(ZMQ.PUSH);
            sender.connect("tcp://localhost:9665");
            String[] testArray = new String[3];
            testArray[0] = "HelloFromFlink a b c d e f g h i j k l m n o p q r s t u v w x y z";
            testArray[1] = "throw_exception";
            testArray[2] = "HelloFromFlink2 a2 b2 c2 d2 e2 f2 g2 h2 i2 j2 k2 l2 m2 n2 o2 p2 q2 r2 s2 t2 u2 v2 w2 x2 y2 z2";

            for(int i = 0; i < testArray.length; i++) {
                System.out.println("Sending: " + testArray[i]);
                sender.send(SerializationTool.serialize(testArray[i]), 0);
            }

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
            env.enableCheckpointing(500);
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000));



            DataStream<String> dataStream = env
                    .addSource((SourceFunction)DSPConnectorFactory.getInstance().createInputOperator(new DSPConnectorConfig.Builder("localhost", 9665)
                            .withSocketType(SocketPool.SocketType.PULL)
                            .withDSP("flink")
                            .build()), TypeInformation.of(String.class))
                    .flatMap(new Splitter(false));

            dataStream.addSink((SinkFunction)DSPConnectorFactory.getInstance().createOutputOperator(new DSPConnectorConfig.Builder("localhost", 9656)
                    .withDSP("flink")
                    .withHWM(500)
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
    public void exceptionPrimarySource1() {
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
            env.enableCheckpointing(500);
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000));

            DataStream<Tuple2<String, Integer>> dataStream = env
                    .addSource((SourceFunction)DSPConnectorFactory.getInstance().createInputOperator(new DSPConnectorConfig.Builder()
                            .withDSP("flink")
                            .withHWM(500)
                            .withInputOperatorFaultTolerance(true)
                            .withRequestAddress("localhost", 9656, DSPConnectorFactory.ConnectorType.PRIMARY)
                            .build()), TypeInfoParser.parse("Tuple2<String,Integer>"))
                    .flatMap(new TupleMapper(true))
                    .keyBy("f0")
                    .timeWindow(Time.seconds(5))
                    .sum("f1");

            dataStream.print();

            env.execute("Window WordCount");
        } catch (Exception ex) {
            System.err.println(ex.toString() + ex.getStackTrace().toString());
        }
    }
}
