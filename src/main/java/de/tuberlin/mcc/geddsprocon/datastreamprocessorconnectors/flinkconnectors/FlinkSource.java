package de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.flinkconnectors;

import de.tuberlin.mcc.geddsprocon.DSPConnectorConfig;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.IDSPSourceConnector;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.SocketPool;
import de.tuberlin.mcc.geddsprocon.messagebuffer.IMessageBufferFunction;
import de.tuberlin.mcc.geddsprocon.messagebuffer.MessageBuffer;
import org.apache.commons.lang.SerializationUtils;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.zeromq.ZMsg;

import java.io.Serializable;

public class FlinkSource implements SourceFunction<Serializable>, IDSPSourceConnector, IMessageBufferFunction {

    private String host;
    private int port;
    private boolean transform;
    private volatile boolean isRunning = true;
    private final DSPConnectorConfig config;
    private final String connectorType;
    private SourceContext<Serializable> ctx;

    public FlinkSource(DSPConnectorConfig config) {
        this.config = config;
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
        while(isRunning) {
            byte[] byteMessage;

            if(config.getSocketType() == SocketPool.SocketType.PULL) {
                while ((byteMessage = receiveData(this.host, this.port)) != null) {

                    Serializable message = (Serializable)SerializationUtils.deserialize(byteMessage);

                    if(message instanceof de.tuberlin.mcc.geddsprocon.tuple.Tuple && this.transform)
                        message = TupleTransformer.transformFromIntermediateTuple((de.tuberlin.mcc.geddsprocon.tuple.Tuple)message);

                    ctx.collect(message);
                }
            } else if(config.getSocketType() == SocketPool.SocketType.REQ || config.getSocketType() == SocketPool.SocketType.DEFAULT) {
                if(!MessageBuffer.getInstance().isEmpty()) {
                    this.ctx = ctx;
                    MessageBuffer.getInstance().flushBuffer(this);
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
    public synchronized ZMsg flush(byte[][] buffer) {
        for(byte[] bytes : buffer) {
            if(bytes.length == 1 && bytes[0] == 0)
                break;

            Serializable message = (Serializable)SerializationUtils.deserialize(bytes);

            if(message instanceof de.tuberlin.mcc.geddsprocon.tuple.Tuple && this.transform)
                message = TupleTransformer.transformFromIntermediateTuple((de.tuberlin.mcc.geddsprocon.tuple.Tuple)message);

            this.ctx.collect(message);
        }

        return null;
    }
}
