package de.tuberlin.mcc.geddsprocon.geddsproconcore.datastreamprocessorconnectors.flinkconnectors;

import de.tuberlin.mcc.geddsprocon.geddsproconcore.DSPConnectorConfig;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.DSPConnectorFactory;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.DSPManager;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.DSPRouter;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.datastreamprocessorconnectors.IDSPSinkConnector;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.datastreamprocessorconnectors.SocketPool;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.messagebuffer.IMessageBufferFunction;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.messagebuffer.MessageBuffer;
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

public class FlinkSink extends RichSinkFunction<Serializable> implements IDSPSinkConnector, IMessageBufferFunction {
    private boolean transform;
    private volatile boolean isRunning = true;
    private final DSPConnectorConfig config;
    private String messageBufferConnectionString;
    private volatile boolean init = false;
    private MessageBuffer messageBuffer;

    private ListState<byte[]> checkpointedState;

    public FlinkSink(DSPConnectorConfig config) {
        this.messageBufferConnectionString = "ipc:///" + config.getBufferConnectionString();
        this.config = config;
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
        if(!init) {
            DSPManager.getInstance().initiateOutputOperator(config, this);
            this.init = true;
        }

        if(this.isRunning && this.init) {
            if(this.transform && value instanceof Tuple)
                value = TupleTransformer.transformToIntermediateTuple((Tuple)value);

            byte[] byteMessage = SerializationUtils.serialize(value);

            // block while the buffer is full
            while(DSPManager.getInstance().getBuffer(this.messageBufferConnectionString).isFull()) {}

            DSPManager.getInstance().getBuffer(this.messageBufferConnectionString).writeBuffer(byteMessage);
        }
    }

    /**
     * Closes the connection with the Socket server.
     */
    @Override
    public void close() {
        this.isRunning = false;
        SocketPool.getInstance().stopSockets(this.config);
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
