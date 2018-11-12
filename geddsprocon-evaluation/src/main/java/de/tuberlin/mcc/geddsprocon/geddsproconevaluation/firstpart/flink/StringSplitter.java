package de.tuberlin.mcc.geddsprocon.geddsproconevaluation.firstpart.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.time.LocalDateTime;

public class StringSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
    private long counter = 0;

    @Override
    public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
        for (String word: sentence.split(" ")) {
            if(word.equals("START_DATA") || word.equals("END_DATA")) {
                BufferedWriter writer = new BufferedWriter(new FileWriter("/home/theanhly/Schreibtisch/test.log", true));
                writer.append(word + ": " + LocalDateTime.now() + "\n");

                if(word.equals("END_DATA")) {
                    System.out.print("SENDING END_DATA");
                    out.collect(new Tuple2<>(word, 1));
                    writer.append("Counter: " + this.counter + "\n" + "==============================\n");
                }

                writer.close();
            } else {
                out.collect(new Tuple2<>(word, 1));
                this.counter++;
            }
        }
    }
}
