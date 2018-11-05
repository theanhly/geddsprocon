package de.tuberlin.mcc.geddsprocon;

import com.google.common.base.Strings;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.SocketPool;
import de.tuberlin.mcc.geddsprocon.messagebuffer.MessageBuffer;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;


public class DSPRequester implements Runnable {
    private String host;
    private int port;
    private String connectorType;
    private int messageNumber;
    private String messageBufferConnectionString;

    public DSPRequester(String host, int port, String connectorType, String messageBufferConnectionString) {
        this.host = host;
        this.port = port;
        this.connectorType = connectorType;
        this.messageBufferConnectionString = messageBufferConnectionString;
        this.messageNumber = -1;
    }

    /**
     * Runs the DSP requester thread.
     */
    @Override
    public void run() {
        ZMQ.Socket socket = SocketPool.getInstance().getOrCreateSocket(this.host, this.port);
        while(true) {

            // alternative way to send a multipart message
            socket.send(this.connectorType, ZMQ.SNDMORE);
            socket.send(Integer.toString(this.messageNumber), ZMQ.DONTWAIT);

            //System.out.println("Trying to receive @" + this.host + ":" + this.port);

            ZMsg messages = ZMsg.recvMsg(socket);

            if(messages != null && !Strings.isNullOrEmpty(messages.peek().toString())) {
                System.out.println("Message received.");
                this.messageNumber = Integer.parseInt(messages.pop().toString());
                for(ZFrame frame : messages) {
                    // block writing to buffer as long the buffer is full
                    while(DSPManager.getInstance().getBuffer(this.messageBufferConnectionString).isFull()) {}

                    DSPManager.getInstance().getBuffer(this.messageBufferConnectionString).writeBuffer(frame.getData());
                }
            }
        }
    }
}
