package de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.flinkconnectors;

import de.tuberlin.mcc.geddsprocon.DSPConnectorConfig;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.IDSPSinkConnector;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.SocketPool;
import de.tuberlin.mcc.geddsprocon.tuple.Tuple2;
import org.apache.commons.lang.SerializationUtils;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.zeromq.ZMQ;

import java.io.Serializable;

public class FlinkSink extends RichSinkFunction<Serializable> implements IDSPSinkConnector {
    // most current host which the sink can send to
    private String host;
    private int port;
    private boolean transform;
    private volatile boolean isRunning = true;
    private ZMQ.Socket socket;
    private final DSPConnectorConfig config;
    private int currentIteration;

    public FlinkSink(DSPConnectorConfig config) {
        this.config = config;
        this.host = config.getHost();
        this.port = config.getPort();
        this.currentIteration = 0;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
    }


    /**
     * Called when new data arrives to the sink, and forwards it to Socket.
     *
     * @param value The value to write to the socket.
     */
    @Override
    public void invoke(Serializable value, Context ctx) throws Exception {
        if(config.getTransform() && value instanceof Tuple)
            value = TupleTransformer.transformToIntermediateTuple((Tuple)value);

        byte[] byteMessage = SerializationUtils.serialize(value);

        sendData(this.host, this.port, byteMessage);
    }

    /**
     * Closes the connection with the Socket server.
     */
    @Override
    public void close() throws Exception {
        this.isRunning = false;

    }

    @Override
    public void sendData(String host, int port, byte[] message) {
        this.currentIteration = SocketPool.getInstance().sendSocket(this.currentIteration, this.config.getAddresses(), message);
    }
}
