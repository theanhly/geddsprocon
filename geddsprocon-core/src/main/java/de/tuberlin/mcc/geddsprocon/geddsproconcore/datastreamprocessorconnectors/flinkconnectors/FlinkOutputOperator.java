package de.tuberlin.mcc.geddsprocon.geddsproconcore.datastreamprocessorconnectors.flinkconnectors;

import de.tuberlin.mcc.geddsprocon.geddsproconcore.DSPConnectorConfig;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.DSPManager;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.common.SerializationTool;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.datastreamprocessorconnectors.IDSPOutputOperator;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.datastreamprocessorconnectors.SocketPool;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.messagebuffer.IMessageBufferFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.zeromq.ZFrame;
import org.zeromq.ZMsg;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

public class FlinkOutputOperator extends RichSinkFunction<Serializable> implements IDSPOutputOperator, IMessageBufferFunction, ListCheckpointed<byte[]> {
    private boolean transform;
    private volatile boolean isRunning = true;
    private final DSPConnectorConfig config;
    private String messageBufferConnectionString;
    private volatile boolean init;

    public FlinkOutputOperator(DSPConnectorConfig config) {
        //this.messageBufferConnectionString = "ipc:///" + config.getBufferConnectionString();
        this.messageBufferConnectionString = config.getHost() + ":" + config.getPort();
        this.config = config;
        this.transform = config.getTransform();
        this.init = false;
    }

    /**
     * initiate the ouput operator
     * @param parameters
     */
    @Override
    public void open(Configuration parameters) {
        synchronized (DSPManager.getInstance().getDspManagerLock()) {
            System.out.println("Output Op @Thread-ID: " + Thread.currentThread().getId() + " Init-Before: " + this.init);
            DSPManager.getInstance().initiateOutputOperator(config, this);
            this.init = true;
            System.out.println("Output Op @Thread-ID: " + Thread.currentThread().getId() + " Init-After: " + this.init);
        }
    }


    /**
     * Called when new data arrives to the sink, and forwards it to Socket.
     *
     * @param value The value to write to the socket.
     */
    @Override
    public void invoke(Serializable value, Context ctx) throws Exception {
        /*synchronized (DSPManager.getInstance().getDspManagerLock()) {
            if(!init) {
                System.out.println("Output Op @Thread-ID: " + Thread.currentThread().getId() + " Init-Before: " + this.init);
                DSPManager.getInstance().initiateOutputOperator(config, this);
                this.init = true;
                System.out.println("Output Op @Thread-ID: " + Thread.currentThread().getId() + " Init-After: " + this.init);
            }
        }*/

        if(this.isRunning && this.init) {
            if(this.transform && value instanceof Tuple)
                value = TupleTransformer.transformToIntermediateTuple((Tuple)value);

            byte[] byteMessage = SerializationTool.serialize(value);

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

    @Override
    public List<byte[]> snapshotState(long checkpointId, long timestamp) throws Exception {
        System.out.println("snapshotState.....");
        ZMsg zmsg = DSPManager.getInstance().getBuffer(this.messageBufferConnectionString).flushBuffer(this, false);
        List<byte[]> list = new LinkedList<>();
        for(ZFrame frame: zmsg )
            list.add(frame.getData());

        return list;
    }

    @Override
    public void restoreState(List<byte[]> state) throws Exception {
        for(byte[] bytes : state) {
            // block while the buffer is full
            while(DSPManager.getInstance().getBuffer(this.messageBufferConnectionString).isFull()) {}

            DSPManager.getInstance().getBuffer(this.messageBufferConnectionString).writeBuffer(bytes);
        }
    }
}
