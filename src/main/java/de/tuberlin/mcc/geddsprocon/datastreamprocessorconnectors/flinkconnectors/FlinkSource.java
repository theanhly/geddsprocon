package de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.flinkconnectors;

import de.tuberlin.mcc.geddsprocon.DSPConnectorConfig;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.IDSPSourceConnector;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.SocketPool;
import org.apache.commons.lang.SerializationUtils;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.io.Serializable;

public class FlinkSource implements SourceFunction<Serializable>, IDSPSourceConnector {

    private String host;
    private int port;
    private boolean transform;
    private volatile boolean isRunning = true;
    private final DSPConnectorConfig config;
    private final String connectorType;

    public FlinkSource(DSPConnectorConfig config) {
        this.config = config;
        this.host = this.config.getHost();
        this.port = this.config.getPort();
        this.transform = this.config.getTransform();
        this.connectorType = this.config.getConncetorType();
    }

    @Override
    public synchronized void run(SourceContext<Serializable> ctx) throws Exception {
       collect(ctx);
        /* while(isRunning) {
            byte[] byteMessage;

            /*
            while (this.isRunning && (byteMessage = receiveData(this.host, this.port)) != null) {

                Serializable message = (Serializable)SerializationUtils.deserialize(byteMessage);

                if(message instanceof de.tuberlin.mcc.geddsprocon.tuple.Tuple && this.transform)
                    message = TupleTransformer.transformFromIntermediateTuple((de.tuberlin.mcc.geddsprocon.tuple.Tuple)message);

                ctx.collect(message);
            }

            if(config.getSocketType() == SocketPool.SocketType.PULL) {
                while ((byteMessage = receiveData(this.host, this.port)) != null) {

                    Serializable message = (Serializable)SerializationUtils.deserialize(byteMessage);

                    if(message instanceof de.tuberlin.mcc.geddsprocon.tuple.Tuple && this.transform)
                        message = TupleTransformer.transformFromIntermediateTuple((de.tuberlin.mcc.geddsprocon.tuple.Tuple)message);

                    ctx.collect(message);
                }
            } else if(config.getSocketType() == SocketPool.SocketType.REQ || config.getSocketType() == SocketPool.SocketType.DEFAULT) {
                ZMQ.Socket socket = SocketPool.getInstance().getOrCreateSocket(this.host, this.port);

                socket.send(this.connectorType);

                System.out.println("Trying to receive");

                ZMsg messages = ZMsg.recvMsg(socket);

                for(ZFrame frame : messages) {
                    Serializable message = (Serializable)SerializationUtils.deserialize(frame.getData());

                    if(message instanceof de.tuberlin.mcc.geddsprocon.tuple.Tuple && transform)
                        message = (Serializable)TupleTransformer.transformFromIntermediateTuple((de.tuberlin.mcc.geddsprocon.tuple.Tuple)message);

                    ctx.collect(message);
                }
            }
        } */
    }

    private synchronized void collect(SourceContext<Serializable> ctx) {
        while(isRunning) {
            byte[] byteMessage;

            /*
            while (this.isRunning && (byteMessage = receiveData(this.host, this.port)) != null) {

                Serializable message = (Serializable)SerializationUtils.deserialize(byteMessage);

                if(message instanceof de.tuberlin.mcc.geddsprocon.tuple.Tuple && this.transform)
                    message = TupleTransformer.transformFromIntermediateTuple((de.tuberlin.mcc.geddsprocon.tuple.Tuple)message);

                ctx.collect(message);
            } */

            if(config.getSocketType() == SocketPool.SocketType.PULL) {
                while ((byteMessage = receiveData(this.host, this.port)) != null) {

                    Serializable message = (Serializable)SerializationUtils.deserialize(byteMessage);

                    if(message instanceof de.tuberlin.mcc.geddsprocon.tuple.Tuple && this.transform)
                        message = TupleTransformer.transformFromIntermediateTuple((de.tuberlin.mcc.geddsprocon.tuple.Tuple)message);

                    ctx.collect(message);
                }
            } else if(config.getSocketType() == SocketPool.SocketType.REQ || config.getSocketType() == SocketPool.SocketType.DEFAULT) {
                ZMQ.Socket socket = SocketPool.getInstance().getOrCreateSocket(this.host, this.port);

                socket.send(this.connectorType);

                System.out.println("Trying to receive");

                ZMsg messages = ZMsg.recvMsg(socket);

                for(ZFrame frame : messages) {
                    Serializable message = (Serializable)SerializationUtils.deserialize(frame.getData());

                    if(message instanceof de.tuberlin.mcc.geddsprocon.tuple.Tuple && transform)
                        message = (Serializable)TupleTransformer.transformFromIntermediateTuple((de.tuberlin.mcc.geddsprocon.tuple.Tuple)message);

                    ctx.collect(message);
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
}
