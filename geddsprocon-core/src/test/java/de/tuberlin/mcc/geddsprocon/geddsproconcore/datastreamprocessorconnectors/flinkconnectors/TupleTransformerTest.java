package de.tuberlin.mcc.geddsprocon.geddsproconcore.datastreamprocessorconnectors.flinkconnectors;


import org.junit.Assert;
import org.junit.Test;

public class TupleTransformerTest {

    @Test
    public void transformFromIntermediateTuplesTest() {
        // Tuple2
        de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple2<String, Integer> tuple2 = new de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple2<>("Hallo Welt", 2);

        org.apache.flink.api.java.tuple.Tuple resultTuple2 = TupleTransformer.transformFromIntermediateTuple(tuple2);

        Assert.assertEquals(resultTuple2.getArity(), 2);
        Assert.assertTrue(resultTuple2 instanceof org.apache.flink.api.java.tuple.Tuple2);
        Assert.assertTrue(((org.apache.flink.api.java.tuple.Tuple2)resultTuple2).f0 instanceof String);
        Assert.assertEquals("Hallo Welt", ((org.apache.flink.api.java.tuple.Tuple2)resultTuple2).f0);
        Assert.assertTrue(((org.apache.flink.api.java.tuple.Tuple2)resultTuple2).f1 instanceof Integer);
        Assert.assertEquals(2, ((org.apache.flink.api.java.tuple.Tuple2)resultTuple2).f1);


        // Tuple10
        de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple10<String, Integer, Double, Float, Boolean, String, Character, Long, String, Integer> tuple10 = new de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple10<>("Ich bin Tom Mustermann", 1337, 2.3332323, 2.11111f, true, "Test", 'T', 21212121212L, "TU Berlin", 42);

        org.apache.flink.api.java.tuple.Tuple resultTuple10 = TupleTransformer.transformFromIntermediateTuple(tuple10);

        Assert.assertEquals(resultTuple10.getArity(), 10);
        Assert.assertTrue(resultTuple10 instanceof org.apache.flink.api.java.tuple.Tuple10);
        Assert.assertTrue(((org.apache.flink.api.java.tuple.Tuple10)resultTuple10).f0 instanceof String);
        Assert.assertEquals("Ich bin Tom Mustermann", ((org.apache.flink.api.java.tuple.Tuple10)resultTuple10).f0);
        Assert.assertTrue(((org.apache.flink.api.java.tuple.Tuple10)resultTuple10).f1 instanceof Integer);
        Assert.assertEquals(1337, ((org.apache.flink.api.java.tuple.Tuple10)resultTuple10).f1);
        Assert.assertTrue(((org.apache.flink.api.java.tuple.Tuple10)resultTuple10).f2 instanceof Double);
        Assert.assertEquals(2.3332323, ((org.apache.flink.api.java.tuple.Tuple10)resultTuple10).f2);
        Assert.assertTrue(((org.apache.flink.api.java.tuple.Tuple10)resultTuple10).f3 instanceof Float);
        Assert.assertEquals(2.11111f, ((org.apache.flink.api.java.tuple.Tuple10)resultTuple10).f3);
        Assert.assertTrue(((org.apache.flink.api.java.tuple.Tuple10)resultTuple10).f4 instanceof Boolean);
        Assert.assertTrue((Boolean)((org.apache.flink.api.java.tuple.Tuple10)resultTuple10).f4);
        Assert.assertTrue(((org.apache.flink.api.java.tuple.Tuple10)resultTuple10).f5 instanceof String);
        Assert.assertEquals("Test", ((org.apache.flink.api.java.tuple.Tuple10)resultTuple10).f5);
        Assert.assertTrue(((org.apache.flink.api.java.tuple.Tuple10)resultTuple10).f6 instanceof Character);
        Assert.assertEquals('T', ((org.apache.flink.api.java.tuple.Tuple10)resultTuple10).f6);
        Assert.assertTrue(((org.apache.flink.api.java.tuple.Tuple10)resultTuple10).f7 instanceof Long);
        Assert.assertEquals(21212121212L, ((org.apache.flink.api.java.tuple.Tuple10)resultTuple10).f7);
        Assert.assertTrue(((org.apache.flink.api.java.tuple.Tuple10)resultTuple10).f8 instanceof String);
        Assert.assertEquals("TU Berlin", ((org.apache.flink.api.java.tuple.Tuple10)resultTuple10).f8);
        Assert.assertTrue(((org.apache.flink.api.java.tuple.Tuple10)resultTuple10).f9 instanceof Integer);
        Assert.assertEquals(42, ((org.apache.flink.api.java.tuple.Tuple10)resultTuple10).f9);
    }

    @Test
    public void transfromToIntermediateTuplesTest() {
        // Tuple2
        org.apache.flink.api.java.tuple.Tuple2<String, Integer> tuple2 = new org.apache.flink.api.java.tuple.Tuple2<>("Hallo Welt", 2);

        de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple resultTuple2 = TupleTransformer.transformToIntermediateTuple(tuple2);

        Assert.assertEquals(resultTuple2.getArity(), 2);
        Assert.assertTrue(resultTuple2 instanceof de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple2);
        Assert.assertTrue(((de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple2)resultTuple2).f_0 instanceof String);
        Assert.assertEquals("Hallo Welt", ((de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple2)resultTuple2).f_0);
        Assert.assertTrue(((de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple2)resultTuple2).f_1 instanceof Integer);
        Assert.assertEquals(2, ((de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple2)resultTuple2).f_1);


        // Tuple10
        org.apache.flink.api.java.tuple.Tuple10<String, Integer, Double, Float, Boolean, String, Character, Long, String, Integer> tuple10 = new org.apache.flink.api.java.tuple.Tuple10<>("Ich bin Tom Mustermann", 1337, 2.3332323, 2.11111f, true, "Test", 'T', 21212121212L, "TU Berlin", 42);

        de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple resultTuple10 = TupleTransformer.transformToIntermediateTuple(tuple10);

        Assert.assertEquals(resultTuple10.getArity(), 10);
        Assert.assertTrue(resultTuple10 instanceof de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple10);
        Assert.assertTrue(((de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple10)resultTuple10).f_0 instanceof String);
        Assert.assertEquals("Ich bin Tom Mustermann", ((de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple10)resultTuple10).f_0);
        Assert.assertTrue(((de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple10)resultTuple10).f_1 instanceof Integer);
        Assert.assertEquals(1337, ((de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple10)resultTuple10).f_1);
        Assert.assertTrue(((de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple10)resultTuple10).f_2 instanceof Double);
        Assert.assertEquals(2.3332323, ((de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple10)resultTuple10).f_2);
        Assert.assertTrue(((de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple10)resultTuple10).f_3 instanceof Float);
        Assert.assertEquals(2.11111f, ((de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple10)resultTuple10).f_3);
        Assert.assertTrue(((de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple10)resultTuple10).f_4 instanceof Boolean);
        Assert.assertTrue((Boolean)((de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple10)resultTuple10).f_4);
        Assert.assertTrue(((de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple10)resultTuple10).f_5 instanceof String);
        Assert.assertEquals("Test", ((de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple10)resultTuple10).f_5);
        Assert.assertTrue(((de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple10)resultTuple10).f_6 instanceof Character);
        Assert.assertEquals('T', ((de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple10)resultTuple10).f_6);
        Assert.assertTrue(((de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple10)resultTuple10).f_7 instanceof Long);
        Assert.assertEquals(21212121212L, ((de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple10)resultTuple10).f_7);
        Assert.assertTrue(((de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple10)resultTuple10).f_8 instanceof String);
        Assert.assertEquals("TU Berlin", ((de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple10)resultTuple10).f_8);
        Assert.assertTrue(((de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple10)resultTuple10).f_9 instanceof Integer);
        Assert.assertEquals(42, ((de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple10)resultTuple10).f_9);
    }
}
