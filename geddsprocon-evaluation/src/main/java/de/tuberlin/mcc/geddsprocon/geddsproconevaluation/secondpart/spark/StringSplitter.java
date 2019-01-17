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

package de.tuberlin.mcc.geddsprocon.geddsproconevaluation.secondpart.spark;

import org.apache.spark.api.java.function.FlatMapFunction;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Iterator;

public class StringSplitter implements FlatMapFunction<String, String> {
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
    public Iterator<String> call(String s) throws Exception {
        String[] stringArr = s.split( " ");

        this.lines++;
        if(stringArr.length == 1 && (stringArr[0].equals("START_DATA") || stringArr[0].equals("END_DATA"))) {
            BufferedWriter writer = new BufferedWriter(new FileWriter(evaluationPathString + "evaluation-spark.log", true));
            if(stringArr[0].equals("START_DATA"))
                writer.append("===============START SPARK===============\n");

            writer.append(stringArr[0] + ": " + LocalDateTime.now() + "\n");

            if(stringArr[0].equals("END_DATA")) {
                System.out.println("SENDING END_DATA");
                writer.append("Lines: " + this.lines + "\n");
                writer.append("Counter: " + this.counter + "\n" + "===============END SPARK===============\n");
                writer.close();
                return Arrays.asList(stringArr[0].split(" ")).iterator();
            }

            writer.close();
        } else {
            this.counter += stringArr.length;
        }

        return Arrays.asList(stringArr).iterator();
    }
}
