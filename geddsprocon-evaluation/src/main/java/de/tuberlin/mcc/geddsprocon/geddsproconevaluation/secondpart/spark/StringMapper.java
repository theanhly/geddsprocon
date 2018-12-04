package de.tuberlin.mcc.geddsprocon.geddsproconevaluation.secondpart.spark;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;


public class StringMapper implements PairFunction<String, String, Integer> {


    @Override
    public Tuple2<String, Integer> call(String word) throws Exception {
        return new Tuple2<String, Integer>(word, 1);
    }
}
