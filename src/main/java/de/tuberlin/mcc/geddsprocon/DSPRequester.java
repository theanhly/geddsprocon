package de.tuberlin.mcc.geddsprocon;

import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.SocketPool;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.flinkconnectors.TupleTransformer;
import de.tuberlin.mcc.geddsprocon.messagebuffer.IMessageBufferFunction;
import de.tuberlin.mcc.geddsprocon.messagebuffer.MessageBuffer;
import org.apache.commons.lang.SerializationUtils;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.io.Serializable;

public class DSPRequester implements Runnable {
    private String host;
    private int port;
    private String connectorType;

    public DSPRequester(String host, int port, String connectorType) {
        this.host = host;
        this.port = port;
        this.connectorType = connectorType;
    }

    @Override
    public void run() {
        ZMQ.Socket socket = SocketPool.getInstance().getOrCreateSocket(this.host, this.port);
        while(true) {
            socket.send(this.connectorType);

            System.out.println("Trying to receive");

            ZMsg messages = ZMsg.recvMsg(socket);

            for(ZFrame frame : messages) {
                // block writing to buffer as long the buffer is full
                while(MessageBuffer.getInstance().isFull()) {}

                MessageBuffer.getInstance().writeBuffer(frame.getData());
            }
        }
    }
}
