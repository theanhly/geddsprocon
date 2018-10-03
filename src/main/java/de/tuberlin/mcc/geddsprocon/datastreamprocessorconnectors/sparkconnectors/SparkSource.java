package de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.sparkconnectors;

import de.tuberlin.mcc.geddsprocon.DSPConnectorConfig;
import de.tuberlin.mcc.geddsprocon.DSPConnectorFactory;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.IDSPSourceConnector;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.SocketPool;
import org.apache.commons.lang.SerializationUtils;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;
import scala.Tuple1;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple5;
import scala.Tuple6;
import scala.Tuple7;
import scala.Tuple8;
import scala.Tuple9;
import scala.Tuple10;
import scala.Tuple11;
import scala.Tuple12;
import scala.Tuple13;
import scala.Tuple14;
import scala.Tuple15;
import scala.Tuple16;
import scala.Tuple17;
import scala.Tuple18;
import scala.Tuple19;
import scala.Tuple20;
import scala.Tuple21;
import scala.Tuple22;

import java.io.Serializable;

public class SparkSource extends Receiver<Serializable> implements IDSPSourceConnector{

    private String host;
    private int port;
    private boolean transform;
    private volatile boolean isRunning = true;
    private final DSPConnectorConfig config;
    private final String connectorType;

    public SparkSource (DSPConnectorConfig config) {
        super(StorageLevel.MEMORY_AND_DISK_2());
        this.config = config;
        this.host = this.config.getHost();
        this.port = this.config.getPort();
        this.transform = this.config.getTransform();
        this.connectorType = this.config.getConncetorType();
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

            /*while (!isStopped() && (byteMessage = receiveData(this.host, this.port)) != null) {

                Serializable message = (Serializable)SerializationUtils.deserialize(byteMessage);

                if(message instanceof de.tuberlin.mcc.geddsprocon.tuple.Tuple && transform)
                    message = (Serializable)TupleTransformer.transformFromIntermediateTuple((de.tuberlin.mcc.geddsprocon.tuple.Tuple)message);

                store(message);
            }*/

            while (!isStopped()) {
                if(config.getSocketType() == SocketPool.SocketType.PULL) {
                    while((byteMessage = receiveData(this.host, this.port)) != null) {
                        Serializable message = (Serializable)SerializationUtils.deserialize(byteMessage);

                        if(message instanceof de.tuberlin.mcc.geddsprocon.tuple.Tuple && transform)
                            message = (Serializable)TupleTransformer.transformFromIntermediateTuple((de.tuberlin.mcc.geddsprocon.tuple.Tuple)message);

                        store(message);
                    }
                } else if (config.getSocketType() == SocketPool.SocketType.REQ || config.getSocketType() == SocketPool.SocketType.DEFAULT) {
                    ZMQ.Socket socket = SocketPool.getInstance().getOrCreateSocket(this.host, this.port);

                    System.out.println("Send request");

                    socket.send(this.connectorType);

                    System.out.println("Trying to receive");

                    ZMsg messages = ZMsg.recvMsg(socket);

                    for(ZFrame frame : messages) {
                        Serializable message = (Serializable)SerializationUtils.deserialize(frame.getData());

                        if(message instanceof de.tuberlin.mcc.geddsprocon.tuple.Tuple && transform)
                            message = (Serializable)TupleTransformer.transformFromIntermediateTuple((de.tuberlin.mcc.geddsprocon.tuple.Tuple)message);

                        store(message);
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
}
