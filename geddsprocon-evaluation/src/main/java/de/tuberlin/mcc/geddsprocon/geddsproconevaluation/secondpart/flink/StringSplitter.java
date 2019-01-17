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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.time.LocalDateTime;

public class StringSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
    private long counter = 0;
    private long lines = 0;
    private String evaluationPathString;

    public StringSplitter()  {
        this("/home/hadoop/thesis-evaluation/");
    }

    public StringSplitter (String evaluationPathString) {
        this.evaluationPathString = evaluationPathString;
    }

    @Override
    public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
        this.lines++;
        for (String word: sentence.split(" ")) {
            if(word.equals("START_DATA") || word.equals("END_DATA")) {
                BufferedWriter writer = new BufferedWriter(new FileWriter(evaluationPathString + "evaluation.log", true));
                if(word.equals("START_DATA"))
                    writer.append("===============START===============\n");

                writer.append(word + ": " + LocalDateTime.now() + "\n");

                if(word.equals("END_DATA")) {
                    System.out.print("SENDING END_DATA");
                    out.collect(new Tuple2<>(word, 1));
                    writer.append("Lines: " + this.lines + "\n");
                    writer.append("Counter: " + this.counter + "\n" + "===============END===============\n");
                }

                writer.close();
            } else {
                out.collect(new Tuple2<>(word, 1));
                this.counter++;
            }
        }
    }
}
