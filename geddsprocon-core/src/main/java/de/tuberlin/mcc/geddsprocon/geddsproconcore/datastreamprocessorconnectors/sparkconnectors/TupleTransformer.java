package de.tuberlin.mcc.geddsprocon.geddsproconcore.datastreamprocessorconnectors.sparkconnectors;


public class TupleTransformer {

    /**
     * Transform the immediate tuple to a Spark tuple (scala.Product)
     * @param tuple Immediate tuple
     * @return Spark tuple
     */
    public static scala.Product transformFromIntermediateTuple(de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple tuple) {
        int arity = tuple.getArity();
        scala.Product resultTuple = null;

        Object[] tupleElements = new Object[arity];

        for(int i = 0; i < arity; i++) {
            tupleElements[i] = tuple.getField(i);
        }

        switch (arity) {
            case 1: resultTuple = new scala.Tuple1(tupleElements[0]); break;
            case 2: resultTuple = new scala.Tuple2(tupleElements[0], tupleElements[1]); break;
            case 3: resultTuple = new scala.Tuple3(tupleElements[0], tupleElements[1], tupleElements[2]); break;
            case 4: resultTuple = new scala.Tuple4(tupleElements[0], tupleElements[1], tupleElements[2], tupleElements[3]); break;
            case 5: resultTuple = new scala.Tuple5(tupleElements[0], tupleElements[1], tupleElements[2], tupleElements[3], tupleElements[4]); break;
            case 6: resultTuple = new scala.Tuple6(tupleElements[0], tupleElements[1], tupleElements[2], tupleElements[3], tupleElements[4], tupleElements[5]); break;
            case 7: resultTuple = new scala.Tuple7(tupleElements[0], tupleElements[1], tupleElements[2], tupleElements[3], tupleElements[4], tupleElements[5], tupleElements[6]); break;
            case 8: resultTuple = new scala.Tuple8(tupleElements[0], tupleElements[1], tupleElements[2], tupleElements[3], tupleElements[4], tupleElements[5], tupleElements[6], tupleElements[7]); break;
            case 9: resultTuple = new scala.Tuple9(tupleElements[0], tupleElements[1], tupleElements[2], tupleElements[3], tupleElements[4], tupleElements[5], tupleElements[6], tupleElements[7], tupleElements[8]); break;
            case 10: resultTuple = new scala.Tuple10(tupleElements[0], tupleElements[1], tupleElements[2], tupleElements[3], tupleElements[4], tupleElements[5], tupleElements[6], tupleElements[7], tupleElements[8], tupleElements[9]); break;
            case 11: resultTuple = new scala.Tuple11(tupleElements[0], tupleElements[1], tupleElements[2], tupleElements[3], tupleElements[4], tupleElements[5], tupleElements[6], tupleElements[7], tupleElements[8], tupleElements[9], tupleElements[10]); break;
            case 12: resultTuple = new scala.Tuple12(tupleElements[0], tupleElements[1], tupleElements[2], tupleElements[3], tupleElements[4], tupleElements[5], tupleElements[6], tupleElements[7], tupleElements[8], tupleElements[9], tupleElements[10], tupleElements[11]); break;
            case 13: resultTuple = new scala.Tuple13(tupleElements[0], tupleElements[1], tupleElements[2], tupleElements[3], tupleElements[4], tupleElements[5], tupleElements[6], tupleElements[7], tupleElements[8], tupleElements[9], tupleElements[10], tupleElements[11], tupleElements[12]); break;
            case 14: resultTuple = new scala.Tuple14(tupleElements[0], tupleElements[1], tupleElements[2], tupleElements[3], tupleElements[4], tupleElements[5], tupleElements[6], tupleElements[7], tupleElements[8], tupleElements[9], tupleElements[10], tupleElements[11], tupleElements[12], tupleElements[13]); break;
            case 15: resultTuple = new scala.Tuple15(tupleElements[0], tupleElements[1], tupleElements[2], tupleElements[3], tupleElements[4], tupleElements[5], tupleElements[6], tupleElements[7], tupleElements[8], tupleElements[9], tupleElements[10], tupleElements[11], tupleElements[12], tupleElements[13], tupleElements[14]); break;
            case 16: resultTuple = new scala.Tuple16(tupleElements[0], tupleElements[1], tupleElements[2], tupleElements[3], tupleElements[4], tupleElements[5], tupleElements[6], tupleElements[7], tupleElements[8], tupleElements[9], tupleElements[10], tupleElements[11], tupleElements[12], tupleElements[13], tupleElements[14], tupleElements[15]); break;
            case 17: resultTuple = new scala.Tuple17(tupleElements[0], tupleElements[1], tupleElements[2], tupleElements[3], tupleElements[4], tupleElements[5], tupleElements[6], tupleElements[7], tupleElements[8], tupleElements[9], tupleElements[10], tupleElements[11], tupleElements[12], tupleElements[13], tupleElements[14], tupleElements[15], tupleElements[16]); break;
            case 18: resultTuple = new scala.Tuple18(tupleElements[0], tupleElements[1], tupleElements[2], tupleElements[3], tupleElements[4], tupleElements[5], tupleElements[6], tupleElements[7], tupleElements[8], tupleElements[9], tupleElements[10], tupleElements[11], tupleElements[12], tupleElements[13], tupleElements[14], tupleElements[15], tupleElements[16], tupleElements[17]); break;
            case 19: resultTuple = new scala.Tuple19(tupleElements[0], tupleElements[1], tupleElements[2], tupleElements[3], tupleElements[4], tupleElements[5], tupleElements[6], tupleElements[7], tupleElements[8], tupleElements[9], tupleElements[10], tupleElements[11], tupleElements[12], tupleElements[13], tupleElements[14], tupleElements[15], tupleElements[16], tupleElements[17], tupleElements[18]); break;
            case 20: resultTuple = new scala.Tuple20(tupleElements[0], tupleElements[1], tupleElements[2], tupleElements[3], tupleElements[4], tupleElements[5], tupleElements[6], tupleElements[7], tupleElements[8], tupleElements[9], tupleElements[10], tupleElements[11], tupleElements[12], tupleElements[13], tupleElements[14], tupleElements[15], tupleElements[16], tupleElements[17], tupleElements[18], tupleElements[19]); break;
            case 21: resultTuple = new scala.Tuple21(tupleElements[0], tupleElements[1], tupleElements[2], tupleElements[3], tupleElements[4], tupleElements[5], tupleElements[6], tupleElements[7], tupleElements[8], tupleElements[9], tupleElements[10], tupleElements[11], tupleElements[12], tupleElements[13], tupleElements[14], tupleElements[15], tupleElements[16], tupleElements[17], tupleElements[18], tupleElements[19], tupleElements[20]); break;
            case 22: resultTuple = new scala.Tuple22(tupleElements[0], tupleElements[1], tupleElements[2], tupleElements[3], tupleElements[4], tupleElements[5], tupleElements[6], tupleElements[7], tupleElements[8], tupleElements[9], tupleElements[10], tupleElements[11], tupleElements[12], tupleElements[13], tupleElements[14], tupleElements[15], tupleElements[16], tupleElements[17], tupleElements[18], tupleElements[19], tupleElements[20], tupleElements[21]); break;
        }

        return resultTuple;
    }

    /**
     * Transform a Spark tuple (scala.Product) to an intermediate tuple
     * @param tuple Spark tuple
     * @return Intermediate tuple
     */
    public static de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple transformToIntermediateTuple(scala.Product tuple) {

        int arity = tuple.productArity();
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
            resultTuple.setField(tuple.productElement(i), i);
        }

        return resultTuple;
    }
}
