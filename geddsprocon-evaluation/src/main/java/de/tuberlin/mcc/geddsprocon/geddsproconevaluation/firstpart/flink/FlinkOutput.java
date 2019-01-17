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

package de.tuberlin.mcc.geddsprocon.geddsproconevaluation.firstpart.flink;

import de.tuberlin.mcc.geddsprocon.geddsproconcore.DSPConnectorConfig;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.DSPConnectorFactory;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.SocketPool;
import de.tuberlin.mcc.geddsprocon.geddsproconevaluation.common.ZeroMQDataProvider;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class FlinkOutput {

    public static void main(String[] args) {
        try{
            int inputPort = 9665;

            ParameterTool parameters = ParameterTool.fromArgs(args);
            String host = parameters.get("host", "0.0.0.0");
            int outPutPort = Integer.parseInt(parameters.get("port", "9656"));
            int bufferSize = Integer.parseInt(parameters.getRequired("buffer"));
            String evaluationPathString = parameters.get("evaluationPath", "/home/hadoop/thesis-evaluation/");

            String file = "/home/hadoop/thesis-evaluation/amazon_reviews_us_Video_DVD_v1_00.tsv";
            Thread zeroMQDataProviderThread = new Thread(new ZeroMQDataProvider(host, inputPort, file));
            zeroMQDataProviderThread.start();

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            DataStream<Tuple2<String, Integer>> wordStream = env
                    .addSource((SourceFunction)DSPConnectorFactory.getInstance().createInputOperator(new DSPConnectorConfig.Builder("0.0.0.0", inputPort)
                            .withSocketType(SocketPool.SocketType.PULL)
                            .withDSP("flink")
                            .build()), TypeInformation.of(String.class))
                    .flatMap(new StringSplitter(evaluationPathString));

            wordStream.addSink((SinkFunction)DSPConnectorFactory.getInstance().createOutputOperator(new DSPConnectorConfig.Builder()
                    .withRouterAddress("0.0.0.0", outPutPort)
                    .withDSP("flink")
                    .withHWM(bufferSize)
                    .withTimeout(5000)
                    .build()));

            //dataStream.print();

            env.execute("FlinkOutput");
        } catch(Exception ex) {
            System.err.println(ex.toString());
        }
    }
}
