package de.tuberlin.mcc.geddsprocon.geddsproconcore.datastreamprocessorconnectors.flinkconnectors;

import de.tuberlin.mcc.geddsprocon.geddsproconcore.DSPConnectorConfig;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.DSPManager;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.common.SerializationTool;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.datastreamprocessorconnectors.IDSPInputOperator;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.datastreamprocessorconnectors.SocketPool;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.messagebuffer.IMessageBufferFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.zeromq.ZFrame;
import org.zeromq.ZMsg;

import java.io.Serializable;

public class FlinkInputOperator extends RichParallelSourceFunction<Serializable> implements IDSPInputOperator, IMessageBufferFunction {

    private String host;
    private int port;
    private boolean transform;
    private volatile boolean isRunning = true;
    private final DSPConnectorConfig config;
    private final String connectorType;
    private SourceContext<Serializable> ctx;
    private String messageBufferConnectionString;
    private volatile boolean init = false;

    public FlinkInputOperator(DSPConnectorConfig config) {
        this.config = config;
        this.messageBufferConnectionString = "ipc:///" + config.getBufferConnectionString();
        this.host = this.config.getHost();
        this.port = this.config.getPort();
        this.transform = this.config.getTransform();
        this.connectorType = this.config.getConncetorType();
    }

    @Override
    public void run(SourceContext<Serializable> ctx) {
       collect(ctx);
    }

    private synchronized void collect(SourceContext<Serializable> ctx) {
        synchronized (DSPManager.getInstance().getDspManagerLock()) {
            if(!this.init) {
                DSPManager.getInstance().initiateInputOperator(this.config, this);
                this.init = true;
            }

        }

        while(isRunning && this.init) {
            byte[] byteMessage;

            if(config.getSocketType() == SocketPool.SocketType.PULL) {

                while ((byteMessage = receiveData(this.host, this.port)) != null) {

                    Serializable message = (Serializable)SerializationTool.deserialize(byteMessage);

                    if(message instanceof de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple && this.transform)
                        message = TupleTransformer.transformFromIntermediateTuple((de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple)message);

                    ctx.collect(message);
                }
            } else if(config.getSocketType() == SocketPool.SocketType.REQ || config.getSocketType() == SocketPool.SocketType.DEFAULT) {

                if(!DSPManager.getInstance().getBuffer(this).isEmpty()) {
                    this.ctx = ctx;
                    DSPManager.getInstance().getBuffer(this).flushBuffer(this);
                }
            }
        }
    }

    @Override
    public void cancel() {
        this.isRunning = false;

        try {
            SocketPool.getInstance().stopSocket(this.host, this.port);
        } catch (IllegalArgumentException  ex) {
            System.err.println(ex.toString());
        }
    }

    @Override
    public byte[] receiveData(String host, int port) {
        return SocketPool.getInstance().receiveSocket(host, port);
    }

    @Override
    public synchronized ZMsg flush(ZMsg messages) {
        for(ZFrame frame : messages) {

            Serializable message = (Serializable)SerializationTool.deserialize(frame.getData());

            if(message instanceof de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple && this.transform)
                message = TupleTransformer.transformFromIntermediateTuple((de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple)message);

            this.ctx.collect(message);
        }

        return null;
    }
}
