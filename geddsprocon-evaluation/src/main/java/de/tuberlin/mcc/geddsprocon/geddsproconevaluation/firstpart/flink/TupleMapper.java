package de.tuberlin.mcc.geddsprocon.geddsproconevaluation.firstpart.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.time.LocalDateTime;

public class TupleMapper implements FlatMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {
    private long counter = 0;

    @Override
    public void flatMap(Tuple2<String, Integer> inputTuple, Collector<Tuple2<String, Integer>> out) throws Exception {
        if(inputTuple.f0.equals("END_DATA")) {
            BufferedWriter writer = new BufferedWriter(new FileWriter("/home/theanhly/Schreibtisch/test.log", true));
            writer.append(inputTuple.f0 + ": " + LocalDateTime.now() + "\n");

            if(inputTuple.f0.equals("END_DATA"))
                writer.append("Counter: " + this.counter + "\n" + "==============================\n");

            writer.close();
        } else {
            out.collect(inputTuple);
            this.counter++;
        }
    }
}
