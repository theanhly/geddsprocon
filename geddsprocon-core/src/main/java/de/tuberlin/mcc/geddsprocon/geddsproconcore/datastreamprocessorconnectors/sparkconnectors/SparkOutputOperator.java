package de.tuberlin.mcc.geddsprocon.geddsproconcore.datastreamprocessorconnectors.sparkconnectors;

import com.google.common.base.Strings;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.DSPConnectorConfig;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.DSPManager;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.common.SerializationTool;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.datastreamprocessorconnectors.IDSPOutputOperator;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.messagebuffer.IMessageBufferFunction;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.function.VoidFunction;
import org.zeromq.ZMsg;

import java.io.Serializable;

public class SparkOutputOperator<T extends JavaRDDLike> implements IDSPOutputOperator, VoidFunction<T>, IMessageBufferFunction {
    private boolean transform;
    private String messageBufferConnectionString;
    private final DSPConnectorConfig config;
    private volatile boolean init = false;

    public SparkOutputOperator(DSPConnectorConfig config) {
        this.config = config;
        this.transform = config.getTransform();
        this.messageBufferConnectionString = Strings.isNullOrEmpty(config.getBufferConnectionString()) ? this.config.getHost() + ":" + this.config.getPort() : "ipc:///" +  config.getBufferConnectionString();
    }

    @Override
    public void call(T value) throws Exception {
        synchronized (DSPManager.getInstance().getDspManagerLock()) {
            if(!init) {
                DSPManager.getInstance().initiateOutputOperator(this.config, this);
                this.init = true;
            }

            if(init) {
                for(Object rdd : value.collect()) {
                    if(rdd instanceof Serializable) {
                        if(rdd instanceof scala.Product && this.transform)
                            rdd = TupleTransformer.transformToIntermediateTuple((scala.Product)rdd);

                        byte[] byteMessage = SerializationTool.serialize((Serializable)rdd);

                        // block while the buffer is full
                        //while(DSPManager.getInstance().getBuffer(this.messageBufferConnectionString).isFull()) {}
                        while(DSPManager.getInstance().getBuffer(this.messageBufferConnectionString).isFull()) {}

                        //DSPManager.getInstance().getBuffer(this.messageBufferConnectionString).writeBuffer(byteMessage);
                        DSPManager.getInstance().getBuffer(this.messageBufferConnectionString).writeBuffer(byteMessage);
                        //System.out.println("Written to buffer");
                    }
                }
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
