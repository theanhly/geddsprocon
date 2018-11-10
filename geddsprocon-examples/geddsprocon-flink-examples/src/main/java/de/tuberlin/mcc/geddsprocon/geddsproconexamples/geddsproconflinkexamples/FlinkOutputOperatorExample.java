package de.tuberlin.mcc.geddsprocon.geddsproconexamples.geddsproconflinkexamples;

import de.tuberlin.mcc.geddsprocon.geddsproconcore.DSPConnectorConfig;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.DSPConnectorFactory;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.datastreamprocessorconnectors.SocketPool;
import org.apache.commons.lang.SerializationUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.zeromq.ZMQ;

public class FlinkOutputOperatorExample {

    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word: sentence.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ZMQ.Context context = ZMQ.context(1);

        //  Socket to talk to server
        System.out.println("Starting program…");
        ZMQ.Socket sender = context.socket(ZMQ.PUSH);
        sender.connect("tcp://192.168.56.102:9665");
        String[] testArray = new String[1];
        testArray[0] = "HelloFromFlink a b c d e f g h i j k l m n o p q r s t u v w x y z";

        for(int i = 0; i < testArray.length; i++) {
            System.out.println("Sending: " + testArray[i]);
            sender.send(SerializationUtils.serialize(testArray[i]), 0);
        }

        DataStream<String> dataStream = env
                .addSource((SourceFunction)DSPConnectorFactory.getInstance().createInputOperator(new DSPConnectorConfig.Builder("192.168.56.102", 9665)
                        .withSocketType(SocketPool.SocketType.PULL)
                        .withDSP("flink")
                        .build()), TypeInformation.of(String.class))
                .flatMap(new Splitter());

        dataStream.addSink((SinkFunction)DSPConnectorFactory.getInstance().createOutputOperator(new DSPConnectorConfig.Builder("192.168.56.102", 9656)
                .withDSP("flink")
                .withHWM(20)
                .withBufferConnectorString("sendbuffer")
                .withTimeout(10000)
                .build()));

        dataStream.print();
        /*
         * Here, you can start creating your execution plan for Flink.
         *
         * Start with getting some data from the environment, like
         * 	env.readTextFile(textPath);
         *
         * then, transform the resulting DataStream<String> using operations
         * like
         * 	.filter()
         * 	.flatMap()
         * 	.join()
         * 	.coGroup()
         *
         * and many more.
         * Have a look at the programming guide for the Java API:
         *
         * http://flink.apache.org/docs/latest/apis/streaming/index.html
         *
         */

        // execute program
        env.execute("Flink Streaming Java API Skeleton");
    }
}
