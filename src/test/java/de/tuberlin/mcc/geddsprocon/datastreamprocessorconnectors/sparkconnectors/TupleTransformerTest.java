package de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.sparkconnectors;


import org.junit.Assert;
import org.junit.Test;

public class TupleTransformerTest {

    @Test
    public void transformFromIntermediateTuplesTest() {
        // Tuple2
        de.tuberlin.mcc.geddsprocon.tuple.Tuple2<String, Integer> tuple2 = new de.tuberlin.mcc.geddsprocon.tuple.Tuple2<>("Hallo Welt", 2);

        scala.Product resultTuple2 = TupleTransformer.transformFromIntermediateTuple(tuple2);

        Assert.assertEquals(resultTuple2.productArity(), 2);
        Assert.assertTrue(resultTuple2 instanceof scala.Tuple2);
        Assert.assertTrue(((scala.Tuple2)resultTuple2)._1 instanceof String);
        Assert.assertEquals("Hallo Welt", ((scala.Tuple2)resultTuple2)._1);
        Assert.assertTrue(((scala.Tuple2)resultTuple2)._2 instanceof Integer);
        Assert.assertEquals(2, ((scala.Tuple2)resultTuple2)._2);


        // Tuple10
        de.tuberlin.mcc.geddsprocon.tuple.Tuple10<String, Integer, Double, Float, Boolean, String, Character, Long, String, Integer> tuple10 = new de.tuberlin.mcc.geddsprocon.tuple.Tuple10<>("Ich bin Tom Mustermann", 1337, 2.3332323, 2.11111f, true, "Test", 'T', 21212121212L, "TU Berlin", 42);

        scala.Product resultTuple10 = TupleTransformer.transformFromIntermediateTuple(tuple10);

        Assert.assertEquals(resultTuple10.productArity(), 10);
        Assert.assertTrue(resultTuple10 instanceof scala.Tuple10);
        Assert.assertTrue(((scala.Tuple10)resultTuple10)._1() instanceof String);
        Assert.assertEquals("Ich bin Tom Mustermann", ((scala.Tuple10)resultTuple10)._1());
        Assert.assertTrue(((scala.Tuple10)resultTuple10)._2() instanceof Integer);
        Assert.assertEquals(1337, ((scala.Tuple10)resultTuple10)._2());
        Assert.assertTrue(((scala.Tuple10)resultTuple10)._3() instanceof Double);
        Assert.assertEquals(2.3332323, ((scala.Tuple10)resultTuple10)._3());
        Assert.assertTrue(((scala.Tuple10)resultTuple10)._4() instanceof Float);
        Assert.assertEquals(2.11111f, ((scala.Tuple10)resultTuple10)._4());
        Assert.assertTrue(((scala.Tuple10)resultTuple10)._5() instanceof Boolean);
        Assert.assertTrue((Boolean)((scala.Tuple10)resultTuple10)._5());
        Assert.assertTrue(((scala.Tuple10)resultTuple10)._6() instanceof String);
        Assert.assertEquals("Test", ((scala.Tuple10)resultTuple10)._6());
        Assert.assertTrue(((scala.Tuple10)resultTuple10)._7() instanceof Character);
        Assert.assertEquals('T', ((scala.Tuple10)resultTuple10)._7());
        Assert.assertTrue(((scala.Tuple10)resultTuple10)._8() instanceof Long);
        Assert.assertEquals(21212121212L, ((scala.Tuple10)resultTuple10)._8());
        Assert.assertTrue(((scala.Tuple10)resultTuple10)._9() instanceof String);
        Assert.assertEquals("TU Berlin", ((scala.Tuple10)resultTuple10)._9());
        Assert.assertTrue(((scala.Tuple10)resultTuple10)._10() instanceof Integer);
        Assert.assertEquals(42, ((scala.Tuple10)resultTuple10)._10());
    }

    @Test
    public void transfromToIntermediateTuplesTest() {
        // Tuple2
        scala.Tuple2<String, Integer> tuple2 = new scala.Tuple2("Hallo Welt", 2);

        de.tuberlin.mcc.geddsprocon.tuple.Tuple resultTuple2 = TupleTransformer.transformToIntermediateTuple(tuple2);

        Assert.assertEquals(resultTuple2.getArity(), 2);
        Assert.assertTrue(resultTuple2 instanceof de.tuberlin.mcc.geddsprocon.tuple.Tuple2);
        Assert.assertTrue(((de.tuberlin.mcc.geddsprocon.tuple.Tuple2)resultTuple2).f0 instanceof String);
        Assert.assertEquals("Hallo Welt", ((de.tuberlin.mcc.geddsprocon.tuple.Tuple2)resultTuple2).f0);
        Assert.assertTrue(((de.tuberlin.mcc.geddsprocon.tuple.Tuple2)resultTuple2).f1 instanceof Integer);
        Assert.assertEquals(2, ((de.tuberlin.mcc.geddsprocon.tuple.Tuple2)resultTuple2).f1);


        // Tuple10
        scala.Tuple10<String, Integer, Double, Float, Boolean, String, Character, Long, String, Integer> tuple10 = new scala.Tuple10("Ich bin Tom Mustermann", 1337, 2.3332323, 2.11111f, true, "Test", 'T', 21212121212L, "TU Berlin", 42);

        de.tuberlin.mcc.geddsprocon.tuple.Tuple resultTuple10 = TupleTransformer.transformToIntermediateTuple(tuple10);

        Assert.assertEquals(resultTuple10.getArity(), 10);
        Assert.assertTrue(resultTuple10 instanceof de.tuberlin.mcc.geddsprocon.tuple.Tuple10);
        Assert.assertTrue(((de.tuberlin.mcc.geddsprocon.tuple.Tuple10)resultTuple10).f0 instanceof String);
        Assert.assertEquals("Ich bin Tom Mustermann", ((de.tuberlin.mcc.geddsprocon.tuple.Tuple10)resultTuple10).f0);
        Assert.assertTrue(((de.tuberlin.mcc.geddsprocon.tuple.Tuple10)resultTuple10).f1 instanceof Integer);
        Assert.assertEquals(1337, ((de.tuberlin.mcc.geddsprocon.tuple.Tuple10)resultTuple10).f1);
        Assert.assertTrue(((de.tuberlin.mcc.geddsprocon.tuple.Tuple10)resultTuple10).f2 instanceof Double);
        Assert.assertEquals(2.3332323, ((de.tuberlin.mcc.geddsprocon.tuple.Tuple10)resultTuple10).f2);
        Assert.assertTrue(((de.tuberlin.mcc.geddsprocon.tuple.Tuple10)resultTuple10).f3 instanceof Float);
        Assert.assertEquals(2.11111f, ((de.tuberlin.mcc.geddsprocon.tuple.Tuple10)resultTuple10).f3);
        Assert.assertTrue(((de.tuberlin.mcc.geddsprocon.tuple.Tuple10)resultTuple10).f4 instanceof Boolean);
        Assert.assertTrue((Boolean)((de.tuberlin.mcc.geddsprocon.tuple.Tuple10)resultTuple10).f4);
        Assert.assertTrue(((de.tuberlin.mcc.geddsprocon.tuple.Tuple10)resultTuple10).f5 instanceof String);
        Assert.assertEquals("Test", ((de.tuberlin.mcc.geddsprocon.tuple.Tuple10)resultTuple10).f5);
        Assert.assertTrue(((de.tuberlin.mcc.geddsprocon.tuple.Tuple10)resultTuple10).f6 instanceof Character);
        Assert.assertEquals('T', ((de.tuberlin.mcc.geddsprocon.tuple.Tuple10)resultTuple10).f6);
        Assert.assertTrue(((de.tuberlin.mcc.geddsprocon.tuple.Tuple10)resultTuple10).f7 instanceof Long);
        Assert.assertEquals(21212121212L, ((de.tuberlin.mcc.geddsprocon.tuple.Tuple10)resultTuple10).f7);
        Assert.assertTrue(((de.tuberlin.mcc.geddsprocon.tuple.Tuple10)resultTuple10).f8 instanceof String);
        Assert.assertEquals("TU Berlin", ((de.tuberlin.mcc.geddsprocon.tuple.Tuple10)resultTuple10).f8);
        Assert.assertTrue(((de.tuberlin.mcc.geddsprocon.tuple.Tuple10)resultTuple10).f9 instanceof Integer);
        Assert.assertEquals(42, ((de.tuberlin.mcc.geddsprocon.tuple.Tuple10)resultTuple10).f9);
    }
}
