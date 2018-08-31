package de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.sparkconnectors;

import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.IDSPSourceConnector;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.SocketPool;
import org.apache.commons.lang.SerializationUtils;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
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

public class SparkSource<T> extends Receiver<T> implements IDSPSourceConnector{

    private String host;
    private int port;
    private volatile boolean isRunning = true;

    public SparkSource (String host, int port) {
        super(StorageLevel.MEMORY_AND_DISK_2());
        this.host = host;
        this.port = port;
    }

    @Override
    public void onStart() {
        // Start the thread that receives data over a connection
        new Thread(this::startSource).start();
    }

    @Override
    public void onStop() {
        // There is nothing much to do as the thread calling receive()
        // is designed to stop by itself if isStopped() returns false
    }

    @Override
    public void startSource() {
        try {
            while(isRunning) {
                byte[] byteMessage;

                while (this.isRunning && (byteMessage = SocketPool.getInstance().receiveSocket(host, port)) != null) {

                    T message = (T)SerializationUtils.deserialize(byteMessage);

                    // Print the message. For testing purposes
                    System.out.println("Received " + ": [" + message + "]");

                    store(message);
                }
            }
        } catch(Throwable t) {
            restart("Error receiving data", t);
        }
    }

    @Override
    public void stopSource() {

    }
}
