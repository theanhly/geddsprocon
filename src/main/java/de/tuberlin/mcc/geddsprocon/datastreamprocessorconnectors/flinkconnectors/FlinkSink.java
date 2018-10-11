package de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.flinkconnectors;

import de.tuberlin.mcc.geddsprocon.DSPConnectorConfig;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.IDSPSinkConnector;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.SocketPool;
import de.tuberlin.mcc.geddsprocon.messagebuffer.IMessageBufferFunction;
import de.tuberlin.mcc.geddsprocon.messagebuffer.MessageBuffer;
import org.apache.commons.lang.SerializationUtils;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.zeromq.ZMsg;

import java.io.Serializable;

// TODO: consider using ListCheckpointed instead
public class FlinkSink extends RichSinkFunction<Serializable> implements IDSPSinkConnector, IMessageBufferFunction, CheckpointedFunction {
    // most current host which the sink can send to
    private String host;
    private int port;
    private boolean transform;
    private volatile boolean isRunning = true;
    private final DSPConnectorConfig config;

    private ListState<byte[]> checkpointedState;

    public FlinkSink(DSPConnectorConfig config) {
        this.config = config;
        this.host = config.getHost();
        this.port = config.getPort();
        this.transform = config.getTransform();
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
        if(this.transform && value instanceof Tuple)
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
    public void close() {
        this.isRunning = false;
        SocketPool.getInstance().stopSockets(this.config);
    }

    // BufferFunction
    @Override
    public IMessageBufferFunction getBufferFunction() {
        return this;
    }

    @Override
    public ZMsg flush(byte[][] buffer) {
        ZMsg messages = new ZMsg();
        for(byte[] byteMessage : buffer) {
            if(byteMessage.length == 1 && byteMessage[0] == 0)
                break;

            messages.add(byteMessage);
        }

        return messages;
    }

    @Override
    public synchronized void snapshotState(FunctionSnapshotContext context) throws Exception {
        this.checkpointedState.clear();
        byte[][] buffer;

        buffer = MessageBuffer.getInstance().getBuffer();
        for (byte[] bytes : buffer) {
            if(bytes.length == 1 && bytes[0] == (byte)0)
                break;

            this.checkpointedState.add(bytes);
        }
    }

    @Override
    public synchronized void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<byte[]> descriptor =
                new ListStateDescriptor<>(
                        "message buffer elements",
                        TypeInformation.of(new TypeHint<byte[]>() {}));

        this.checkpointedState = context.getOperatorStateStore().getListState(descriptor);

        if (context.isRestored()) {
            MessageBuffer.getInstance().clearBuffer();

            for (byte[] element : this.checkpointedState.get()) {
                MessageBuffer.getInstance().writeBuffer((element));
            }
        }
    }
}
