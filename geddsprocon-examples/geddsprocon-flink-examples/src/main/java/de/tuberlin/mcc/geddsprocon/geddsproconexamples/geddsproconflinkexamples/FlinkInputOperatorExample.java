/*
 * Copyright 2019 The-Anh Ly
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.tuberlin.mcc.geddsprocon.geddsproconexamples.geddsproconflinkexamples;

import de.tuberlin.mcc.geddsprocon.geddsproconcore.DSPConnectorConfig;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.DSPConnectorFactory;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.java.typeutils.TypeInfoParser;

public class FlinkInputOperatorExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> dataStream = env
                .addSource((SourceFunction)DSPConnectorFactory.getInstance().createInputOperator(new DSPConnectorConfig.Builder()
                        .withDSP("flink")
                        .withBufferConnectorString("recvbuffer")
                        .withRequestAddress("192.168.56.102", 9656, DSPConnectorFactory.ConnectorType.SECONDARY)
                        .withRequestAddress("192.168.56.102", 9666, DSPConnectorFactory.ConnectorType.SECONDARY)
                        .build()), TypeInfoParser.parse("Tuple2<String,Integer>"))
                .keyBy("f0")
                .timeWindow(Time.seconds(5))
                .sum("f1");

        dataStream.print();

        env.execute("Window WordCount");
    }
}
