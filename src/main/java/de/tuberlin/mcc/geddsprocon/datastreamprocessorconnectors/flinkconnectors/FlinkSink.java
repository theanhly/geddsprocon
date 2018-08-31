package de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.flinkconnectors;

import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.IDSPSinkConnector;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.SocketPool;
import org.apache.commons.lang.SerializationUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.zeromq.ZMQ;

import java.io.Serializable;

public class FlinkSink<T extends Serializable> extends RichSinkFunction<T> implements IDSPSinkConnector {
    private String host;
    private int port;
    private volatile boolean isRunning = true;
    private ZMQ.Socket socket;

    public FlinkSink(String host, int port) {
        this.host = host;
        this.port = port;
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
    public void invoke(T value, Context ctx) throws Exception {
        byte[] byteMessage = SerializationUtils.serialize(value);
        SocketPool.getInstance().sendSocket(host, port, byteMessage);
    }

    /**
     * Closes the connection with the Socket server.
     */
    @Override
    public void close() throws Exception {
        this.isRunning = false;

    }


    public void startSink() {

    }

    public void stopSink() {

    }
}
