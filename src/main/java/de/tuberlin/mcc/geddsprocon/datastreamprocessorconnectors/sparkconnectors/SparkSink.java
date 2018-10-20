package de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.sparkconnectors;

import de.tuberlin.mcc.geddsprocon.DSPConnectorConfig;
import de.tuberlin.mcc.geddsprocon.DSPConnectorFactory;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.IDSPSinkConnector;
import de.tuberlin.mcc.geddsprocon.messagebuffer.IMessageBufferFunction;
import org.apache.commons.lang.SerializationUtils;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.function.VoidFunction;
import org.zeromq.ZMsg;

import java.io.Serializable;

public class SparkSink<T extends JavaRDDLike> implements IDSPSinkConnector, VoidFunction<T>, IMessageBufferFunction {
    private boolean transform;
    private String messageBufferConnectionString;

    public SparkSink(DSPConnectorConfig config, String messageBufferConnectionString) {
        this.transform = config.getTransform();
        this.messageBufferConnectionString = messageBufferConnectionString;
    }

    @Override
    public void call(T value) throws Exception {
        for(Object rdd : value.collect()) {
            if(rdd instanceof Serializable) {
                if(rdd instanceof scala.Product && transform)
                    rdd = TupleTransformer.transformToIntermediateTuple((scala.Product)rdd);

                byte[] byteMessage = SerializationUtils.serialize((Serializable)rdd);

                // block while the buffer is full
                while(DSPConnectorFactory.getInstance().getBuffer(this.messageBufferConnectionString).isFull()) {}

                DSPConnectorFactory.getInstance().getBuffer(this.messageBufferConnectionString).writeBuffer(byteMessage);
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

        return message;
    }
}
