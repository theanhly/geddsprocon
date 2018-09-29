package de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.flinkconnectors;

import de.tuberlin.mcc.geddsprocon.DSPConnectorConfig;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.IDSPSinkConnector;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.SocketPool;
import de.tuberlin.mcc.geddsprocon.messagebuffer.IMessageBufferFunction;
import de.tuberlin.mcc.geddsprocon.messagebuffer.MessageBuffer;
import org.apache.commons.lang.SerializationUtils;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.zeromq.ZMsg;

import java.io.Serializable;
import java.util.Arrays;

public class FlinkSink extends RichSinkFunction<Serializable> implements IDSPSinkConnector, IMessageBufferFunction{
    // most current host which the sink can send to
    private String host;
    private int port;
    private boolean transform;
    private volatile boolean isRunning = true;
    private final DSPConnectorConfig config;
    private int currentIteration;

    public FlinkSink(DSPConnectorConfig config) {
        this.config = config;
        this.host = config.getHost();
        this.port = config.getPort();
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

        // block while the buffer is full
        while(MessageBuffer.getInstance().isFull()) {}

        MessageBuffer.getInstance().writeBuffer(byteMessage);
        System.out.println("Written to buffer");
    }

    /**
     * Closes the connection with the Socket server.
     */
    @Override
    public void close() throws Exception {
        this.isRunning = false;
        SocketPool.getInstance().stopSockets(this.config);
    }

    @Override
    public void sendData(String host, int port, byte[] message) {
        //this.currentIteration = SocketPool.getInstance().sendSocket(this.currentIteration, this.config.getAddresses(), message);
    }

    @Override
    public ZMsg flush(MessageBuffer messageBuffer) {
        ZMsg messages = new ZMsg();
        for(byte[] byteMessage : messageBuffer.getBuffer()) {
            if(byteMessage.length == 1 && byteMessage[0] == 0)
                break;

            messages.add(byteMessage);
        }

        return messages;
    }
}
