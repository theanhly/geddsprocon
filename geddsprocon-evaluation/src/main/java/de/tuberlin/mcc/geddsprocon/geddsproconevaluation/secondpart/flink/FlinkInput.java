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

package de.tuberlin.mcc.geddsprocon.geddsproconevaluation.secondpart.flink;

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
            String host = parameters.get("host", "0.0.0.0");
            int inputPort = Integer.parseInt(parameters.get("port", "9656"));
            int bufferSize = Integer.parseInt(parameters.getRequired("buffer"));
            boolean requesterFaultTolerance = Boolean.parseBoolean(parameters.get("fault-tolerance", "false"));

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            DataStream<Tuple2<String, Integer>> dataStream = env
                    .addSource((SourceFunction)DSPConnectorFactory.getInstance().createInputOperator(new DSPConnectorConfig.Builder()
                            .withDSP("flink")
                            .withHWM(bufferSize)
                            .withInputOperatorFaultTolerance(requesterFaultTolerance)
                            .withTimeout(15000)
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
