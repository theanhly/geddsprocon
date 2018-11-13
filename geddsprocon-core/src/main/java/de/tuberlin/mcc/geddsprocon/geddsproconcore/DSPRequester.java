package de.tuberlin.mcc.geddsprocon.geddsproconcore;

import com.google.common.base.Strings;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.datastreamprocessorconnectors.SocketPool;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.messagebuffer.MessageBuffer;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;
import org.zeromq.ZMsg;


public class DSPRequester implements Runnable {
    private String host;
    private int port;
    private String connectorType;
    private long messageNumber;
    private String messageBufferConnectionString;
    private MessageBuffer messageBuffer;

    public DSPRequester(String host, int port, String connectorType, MessageBuffer messageBuffer) {
        this.host = host;
        this.port = port;
        this.connectorType = connectorType;
        this.messageBufferConnectionString = messageBufferConnectionString;
        this.messageNumber = -1;
        this.messageBuffer = messageBuffer;
    }

    /**
     * Runs the DSP requester thread.
     */
    @Override
    public void run() {
        try {
            ZMQ.Socket socket = SocketPool.getInstance().getOrCreateSocket(this.host, this.port);
            while(true) {

                //might need to lock until receive. can cause ZMQException where it receives while the socket is used by the other requester.
                // alternative way to send a multipart message

                ZMsg messages = new ZMsg();

                synchronized (DSPManager.getInstance().getDspRequesterLock()) {
                    socket.send(this.connectorType, ZMQ.SNDMORE);
                    socket.send(Long.toString(DSPManager.getInstance().getLastReceivedMessageID()), ZMQ.DONTWAIT);

                    //System.out.println("Trying to receive @" + this.host + ":" + this.port + " with Thread-ID: " + Thread.currentThread().getId());

                    messages = ZMsg.recvMsg(socket);

                    if(messages != null && !Strings.isNullOrEmpty(messages.peek().toString()))
                        DSPManager.getInstance().setLastReceivedMessageID(Long.parseLong(messages.pop().toString()));
                }

                if(messages != null && !Strings.isNullOrEmpty(messages.peek().toString())) {
                    //System.out.println("Message received.");
                    //this.messageNumber = Long.parseLong(messages.pop().toString());
                    for(ZFrame frame : messages) {
                        // block writing to buffer as long the buffer is full
                        while(this.messageBuffer.isFull()) {}

                        //System.out.println(DSPManager.getInstance().getBuffer(this.messageBufferConnectionString).getMessages());
                        this.messageBuffer.writeBuffer(frame.getData());
                    }
                }
            }
        } catch(ZMQException ex) {
            System.err.println("ZMQException in thread " + Thread.currentThread().getId());
            System.err.println(ex.toString());
            System.err.println(ex.getStackTrace());
        }
    }
}
