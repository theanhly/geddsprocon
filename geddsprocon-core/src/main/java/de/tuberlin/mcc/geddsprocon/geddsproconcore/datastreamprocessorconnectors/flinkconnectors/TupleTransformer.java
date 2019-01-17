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

package de.tuberlin.mcc.geddsprocon.geddsproconcore.datastreamprocessorconnectors.flinkconnectors;


public class TupleTransformer {

    /**
     * Transform an intermediate tuple to a Flink tuple
     * @param tuple Immediate tuple
     * @return Flink tuple
     */
    public static org.apache.flink.api.java.tuple.Tuple transformFromIntermediateTuple(de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple tuple) {

        int arity = tuple.getArity();
        org.apache.flink.api.java.tuple.Tuple resultTuple = null;

        switch (arity) {
            case 0: resultTuple = new org.apache.flink.api.java.tuple.Tuple0(); break;
            case 1: resultTuple = new org.apache.flink.api.java.tuple.Tuple1(); break;
            case 2: resultTuple = new org.apache.flink.api.java.tuple.Tuple2(); break;
            case 3: resultTuple = new org.apache.flink.api.java.tuple.Tuple3(); break;
            case 4: resultTuple = new org.apache.flink.api.java.tuple.Tuple4(); break;
            case 5: resultTuple = new org.apache.flink.api.java.tuple.Tuple5(); break;
            case 6: resultTuple = new org.apache.flink.api.java.tuple.Tuple6(); break;
            case 7: resultTuple = new org.apache.flink.api.java.tuple.Tuple7(); break;
            case 8: resultTuple = new org.apache.flink.api.java.tuple.Tuple8(); break;
            case 9: resultTuple = new org.apache.flink.api.java.tuple.Tuple9(); break;
            case 10: resultTuple = new org.apache.flink.api.java.tuple.Tuple10(); break;
            case 11: resultTuple = new org.apache.flink.api.java.tuple.Tuple11(); break;
            case 12: resultTuple = new org.apache.flink.api.java.tuple.Tuple12(); break;
            case 13: resultTuple = new org.apache.flink.api.java.tuple.Tuple13(); break;
            case 14: resultTuple = new org.apache.flink.api.java.tuple.Tuple14(); break;
            case 15: resultTuple = new org.apache.flink.api.java.tuple.Tuple15(); break;
            case 16: resultTuple = new org.apache.flink.api.java.tuple.Tuple16(); break;
            case 17: resultTuple = new org.apache.flink.api.java.tuple.Tuple17(); break;
            case 18: resultTuple = new org.apache.flink.api.java.tuple.Tuple18(); break;
            case 19: resultTuple = new org.apache.flink.api.java.tuple.Tuple19(); break;
            case 20: resultTuple = new org.apache.flink.api.java.tuple.Tuple20(); break;
            case 21: resultTuple = new org.apache.flink.api.java.tuple.Tuple21(); break;
            case 22: resultTuple = new org.apache.flink.api.java.tuple.Tuple22(); break;
            case 23: resultTuple = new org.apache.flink.api.java.tuple.Tuple23(); break;
            case 24: resultTuple = new org.apache.flink.api.java.tuple.Tuple24(); break;
            case 25: resultTuple = new org.apache.flink.api.java.tuple.Tuple25(); break;
        }

        for(int i = 0; i < arity; i++) {
            resultTuple.setField(tuple.getField(i), i);
        }

        return resultTuple;
    }

    /**
     * Transform a Flink tuple to an intermediate tuple
     * @param tuple Flink tuple
     * @return Intermediate tuple
     */
    public static de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple transformToIntermediateTuple(org.apache.flink.api.java.tuple.Tuple tuple) {

        int arity = tuple.getArity();
        de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple resultTuple = null;

        switch (arity) {
            case 0: resultTuple = new de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple0(); break;
            case 1: resultTuple = new de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple1(); break;
            case 2: resultTuple = new de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple2(); break;
            case 3: resultTuple = new de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple3(); break;
            case 4: resultTuple = new de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple4(); break;
            case 5: resultTuple = new de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple5(); break;
            case 6: resultTuple = new de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple6(); break;
            case 7: resultTuple = new de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple7(); break;
            case 8: resultTuple = new de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple8(); break;
            case 9: resultTuple = new de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple9(); break;
            case 10: resultTuple = new de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple10(); break;
            case 11: resultTuple = new de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple11(); break;
            case 12: resultTuple = new de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple12(); break;
            case 13: resultTuple = new de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple13(); break;
            case 14: resultTuple = new de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple14(); break;
            case 15: resultTuple = new de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple15(); break;
            case 16: resultTuple = new de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple16(); break;
            case 17: resultTuple = new de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple17(); break;
            case 18: resultTuple = new de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple18(); break;
            case 19: resultTuple = new de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple19(); break;
            case 20: resultTuple = new de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple20(); break;
            case 21: resultTuple = new de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple21(); break;
            case 22: resultTuple = new de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple22(); break;
            case 23: resultTuple = new de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple23(); break;
            case 24: resultTuple = new de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple24(); break;
            case 25: resultTuple = new de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple25(); break;
        }

        for(int i = 0; i < arity; i++) {
            resultTuple.setField(tuple.getField(i), i);
        }

        return resultTuple;
    }
}
