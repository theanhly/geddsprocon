package de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.flinkconnectors;

import de.tuberlin.mcc.geddsprocon.DSPConnectorFactory;
import org.apache.commons.lang.SerializationUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
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
    public void serializerTest() {
        Tuple4<String, Integer, Double, Boolean> test = new Tuple4<String, Integer, Double, Boolean>("test", 2, 0.2323123541, false);


        byte[] testByteArr = SerializationUtils.serialize(test);

        Tuple4 testObj = (Tuple4)SerializationUtils.deserialize(testByteArr);

        Assert.assertEquals("test", testObj.f0);
        Assert.assertEquals(2, testObj.f1);
        Assert.assertEquals(0.2323123541, testObj.f2);
        Assert.assertFalse((Boolean)testObj.f3);
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

            DSPConnectorFactory<String> dspConnectorFactory = new DSPConnectorFactory<>();
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            DataStream<Tuple2<String, Integer>> dataStream = env
                    //.socketTextStream("localhost", 8080)
                    .addSource((SourceFunction)dspConnectorFactory.createSourceConnector(DSPConnectorFactory.DataStreamProcessors.FLINK, "localhost", 5555), TypeInformation.of(String.class))
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
                    .addSource((SourceFunction)new DSPConnectorFactory().createSourceConnector(DSPConnectorFactory.DataStreamProcessors.FLINK, "localhost", 5555), TypeInformation.of(String.class))
                    .flatMap(new Splitter())
                    .keyBy(0)
                    .timeWindow(Time.seconds(5))
                    .sum(1);

            dataStream.addSink((SinkFunction)new DSPConnectorFactory().createSinkConnector(DSPConnectorFactory.DataStreamProcessors.FLINK, "localhost", 5556));

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
                    .addSource((SourceFunction)dspConnectorFactory.createSourceConnector(DSPConnectorFactory.DataStreamProcessors.FLINK, "localhost", 5555), TypeInformation.of(String.class))
                    .flatMap(new Splitter());

            dataStream.addSink((SinkFunction)dspSinkConnectorFactory.createSinkConnector(DSPConnectorFactory.DataStreamProcessors.FLINK, "localhost", 5556));

            dataStream.print();

            env.execute("Window WordCount");
        } catch (Exception ex) {
            System.err.println(ex.getStackTrace());
        }
    }

    @Test
    public void pipelineTest1b() {
        try {
            ZMQ.Context context = ZMQ.context(1);

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            DataStream<Tuple2<String, Integer>> dataStream = env
                    .addSource((SourceFunction)new DSPConnectorFactory<>().createSourceConnector(DSPConnectorFactory.DataStreamProcessors.FLINK, "localhost", 5556), TypeInfoParser.parse("Tuple2<String,Integer>"))
                    .keyBy(0)
                    .timeWindow(Time.seconds(5))
                    .sum(1);

            dataStream.print();

            env.execute("Window WordCount");
        } catch (Exception ex) {
            System.err.println(ex.toString() + ex.getStackTrace().toString());
        }
    }
    // ======== Pipeline test finished ========
}
