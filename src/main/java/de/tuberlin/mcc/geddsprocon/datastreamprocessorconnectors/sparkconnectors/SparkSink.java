package de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.sparkconnectors;

import de.tuberlin.mcc.geddsprocon.DSPConnectorConfig;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.IDSPSinkConnector;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.SocketPool;
import de.tuberlin.mcc.geddsprocon.messagebuffer.IMessageBufferFunction;
import de.tuberlin.mcc.geddsprocon.messagebuffer.MessageBuffer;
import org.apache.commons.lang.SerializationUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.function.VoidFunction;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.io.Serializable;

public class SparkSink<T extends JavaRDDLike> implements IDSPSinkConnector, VoidFunction<T>, IMessageBufferFunction {
    private String host;
    private int port;
    private boolean transform;
    private final DSPConnectorConfig config;
    private int currentIteration;

    public SparkSink(DSPConnectorConfig config) {
        this.host = config.getHost();
        this.port = config.getPort();
        this.transform = config.getTransform();
        this.config = config;
        this.currentIteration = 0;
    }

    @Override
    public void call(T value) throws Exception {
        for(Object rdd : value.collect()) {
            if(rdd instanceof Serializable) {
                if(rdd instanceof scala.Product && transform)
                    rdd = TupleTransformer.transformToIntermediateTuple((scala.Product)rdd);

                byte[] byteMessage = SerializationUtils.serialize((Serializable)rdd);

                // block while the buffer is full
                while(MessageBuffer.getInstance().isFull()) {}

                MessageBuffer.getInstance().writeBuffer(byteMessage);
                System.out.println("Written to buffer");
            }
        }
    }

    @Override
    public IMessageBufferFunction getBufferFunction() {
        return this;
    }

    @Override
    public ZMsg flush(ZMsg message) {
        /*ZMsg messages = new ZMsg();
        for(byte[] byteMessage : buffer) {
            if(byteMessage.length == 1 && byteMessage[0] == 0)
                break;

            messages.add(byteMessage);
        }*/

        return message;
    }
}
