package de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.flinkconnectors;

import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.IDSPSourceConnector;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.SocketPool;
import org.apache.commons.lang.SerializationUtils;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.zeromq.ZMQ;

public class FlinkSource<T> implements SourceFunction<T>, IDSPSourceConnector {

    private String host;
    private int port;
    private volatile boolean isRunning = true;

    public FlinkSource(String host, int port) {
        this.host = host;
        this.port = port;
    }

    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        //startSource();
        while(isRunning) {
            byte[] byteMessage;

            while (this.isRunning && (byteMessage = SocketPool.getInstance().receiveSocket(host, port)) != null) {

                T message = (T)SerializationUtils.deserialize(byteMessage);

                // Print the message. For testing purposes
                System.out.println("Received " + ": [" + message + "]");

                ctx.collect(message);
            }
        }
    }

    @Override
    public void cancel() {
        stopSource();
    }

    @Override
    public void startSource() {
    }

    @Override
    public void stopSource() {
        this.isRunning = false;

        try {
            SocketPool.getInstance().getSocket(host, port).close();
        } catch (IllegalArgumentException   ex) {
            System.err.println(ex.toString());
        }
    }
}
