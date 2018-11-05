package de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.sparkconnectors;

import de.tuberlin.mcc.geddsprocon.DSPConnectorConfig;
import de.tuberlin.mcc.geddsprocon.DSPConnectorFactory;
import de.tuberlin.mcc.geddsprocon.DSPManager;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.IDSPSourceConnector;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.SocketPool;
import de.tuberlin.mcc.geddsprocon.messagebuffer.IMessageBufferFunction;
import org.apache.commons.lang.SerializationUtils;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.zeromq.ZFrame;
import org.zeromq.ZMsg;

import java.io.Serializable;

public class SparkSource extends Receiver<Serializable> implements IDSPSourceConnector, IMessageBufferFunction {

    private String host;
    private int port;
    private boolean transform;
    private final DSPConnectorConfig config;
    private String messageBufferConnectionString;
    private volatile boolean init = false;

    public SparkSource (DSPConnectorConfig config) {
        super(StorageLevel.MEMORY_AND_DISK_2());
        this.messageBufferConnectionString = "ipc:///" + config.getBufferConnectionString();
        this.config = config;
        this.host = this.config.getHost();
        this.port = this.config.getPort();
        this.transform = this.config.getTransform();
    }

    @Override
    public void onStart() {
        // Start the thread that receives data over a connection
        new Thread(this::startSource).start();
    }

    @Override
    public void onStop() {
        if(isStopped())
            SocketPool.getInstance().stopSocket(this.host, this.port);

    }

    public synchronized void startSource() {
        try {
            byte[] byteMessage;

            if(!this.init) {
                DSPManager.getInstance().initiateInputOperator(this.config);
                this.init = true;
            }

            while (!isStopped() && this.init) {
                if(config.getSocketType() == SocketPool.SocketType.PULL) {
                    while((byteMessage = receiveData(this.host, this.port)) != null) {
                        Serializable message = (Serializable)SerializationUtils.deserialize(byteMessage);

                        if(message instanceof de.tuberlin.mcc.geddsprocon.tuple.Tuple && transform)
                            message = (Serializable)TupleTransformer.transformFromIntermediateTuple((de.tuberlin.mcc.geddsprocon.tuple.Tuple)message);

                        store(message);
                    }
                } else if (config.getSocketType() == SocketPool.SocketType.REQ || config.getSocketType() == SocketPool.SocketType.DEFAULT) {
                    if(!DSPManager.getInstance().getBuffer(this.messageBufferConnectionString).isEmpty()) {
                        DSPManager.getInstance().getBuffer(this.messageBufferConnectionString).flushBuffer(this);
                    }
                }
            }

        } catch(Throwable t) {
            restart("Error receiving data", t);
        }
    }

    @Override
    public byte[] receiveData(String host, int port) {
        return SocketPool.getInstance().receiveSocket(host, port);
    }

    @Override
    public synchronized ZMsg flush(ZMsg messages) {
        for(ZFrame frame : messages) {

            Serializable message = (Serializable)SerializationUtils.deserialize(frame.getData());

            if(message instanceof de.tuberlin.mcc.geddsprocon.tuple.Tuple && this.transform)
                message = (Serializable)TupleTransformer.transformFromIntermediateTuple((de.tuberlin.mcc.geddsprocon.tuple.Tuple)message);

            store(message);
        }

        return null;
    }
}
