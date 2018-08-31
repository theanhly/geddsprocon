package de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.sparkconnectors;

import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.IDSPSinkConnector;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.SocketPool;
import org.apache.commons.lang.SerializationUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.function.VoidFunction;

import java.io.Serializable;

public class SparkSink<T extends JavaRDDLike> implements IDSPSinkConnector, VoidFunction<T> {
    private String host;
    private int port;

    public SparkSink(String host, int port) {
        this.host = host;
        this.port = port;
    }

    @Override
    public void startSink() {

    }

    @Override
    public void stopSink() {

    }

    @Override
    public void call(T value) throws Exception {
        for(Object rdd : value.collect()) {
            if(rdd instanceof Serializable) {
                byte[] byteMessage = SerializationUtils.serialize((Serializable)rdd);
                SocketPool.getInstance().sendSocket(host, port, byteMessage);
            }
        }
    }
}
