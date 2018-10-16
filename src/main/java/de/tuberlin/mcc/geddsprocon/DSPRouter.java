package de.tuberlin.mcc.geddsprocon;

import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.SocketPool;
import de.tuberlin.mcc.geddsprocon.messagebuffer.IMessageBufferFunction;
import de.tuberlin.mcc.geddsprocon.messagebuffer.IMessageBufferListener;
import de.tuberlin.mcc.geddsprocon.messagebuffer.MessageBuffer;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.util.HashSet;
import java.util.LinkedList;

public class DSPRouter implements Runnable, IMessageBufferListener {

    private String host;
    private int port;
    private volatile IMessageBufferFunction bufferFunction;
    private volatile LinkedList<ZFrame> endpointQueue;
    private volatile HashSet<ZFrame> endpointSet;
    private final ZMQ.Socket socket;
    private ZFrame temporaryPrimary;

    public DSPRouter(String host, int port, IMessageBufferFunction bufferFunction) {
        this.host = host;
        this.port = port;
        this.bufferFunction = bufferFunction;
        this.endpointQueue = new LinkedList<>();
        this.endpointSet = new HashSet<>();
        this.socket = SocketPool.getInstance().getOrCreateSocket(this.host, this.port);
    }

    @Override
    public void run() {
        while(true) {

            ZMsg msg = ZMsg.recvMsg(this.socket);
            System.out.println("message received");

            //  First frame is address
            ZFrame address = msg.pop();

            //  Second frame is empty in a REQ socket. Second frame of DEALER socket is not empty
            //String empty = new String(msg.pop().getData());
            //assert (empty.length() == 0);

            String ready = new String(msg.pop().getData());
            assert(ready.length() > 0);

            if(ready.equals(DSPConnectorFactory.ConnectorType.PRIMARY)) {
                System.out.println(DSPConnectorFactory.ConnectorType.PRIMARY + " message received");
                if(!reply(address)) {
                    //reply();
                    System.out.println("Message could not be sent.");
                } else {
                    this.temporaryPrimary = null;
                }
            } else if(ready.equals(DSPConnectorFactory.ConnectorType.SECONDARY)) {
                System.out.println(DSPConnectorFactory.ConnectorType.SECONDARY +" message received");
                // critical

                if(this.temporaryPrimary != null && this.temporaryPrimary.hasData() && this.temporaryPrimary.getData().equals(address.getData()))
                    reply(address);
                else if(this.endpointSet.add(address))
                    this.endpointQueue.add(address);
            }
        }
    }

    /**
     * go through the queue and send to the addresses in the queue
     * @return return true if sending was successful
     */
    private boolean reply() {
        ZFrame addressFrame;
        do {
            addressFrame = this.endpointQueue.pop();
            this.temporaryPrimary = addressFrame.duplicate();
            this.endpointSet.remove(addressFrame);
            System.out.println("Address popped");
        } while(!reply(addressFrame));

        System.out.println("Reply sent");
        return true;
    }

    /**
     * reply to the address by flushing the message buffer
     * @param address
     * @return
     */
    private boolean reply(ZFrame address) {
        ZMsg message = new ZMsg();
        message.add(address);

        // DEALER socket doesn't need an empty second frame
        //message.add("");

        while(MessageBuffer.getInstance().isEmpty()) {}

        // TODO: critical error. the buffer is flushed but it might be the case that in the meantime new values could have been written into the buffer.
        Object bufferLock = MessageBuffer.getInstance().getBufferLock();
        synchronized (bufferLock) {
            message.append(MessageBuffer.getInstance().flushBuffer(this.bufferFunction, false));
            System.out.println("Sending message " + message.toString());
            if(message.send(this.socket)) {
                MessageBuffer.getInstance().clearBuffer();
                return true;
            } else {
                System.err.println("Sending not successful.");
                return false;
            }
        }
    }

    /**
     * is used when the buffer is full. go through the queue and find an address to reply to
     */
    @Override
    public void bufferIsFullEvent() {
        while(this.endpointQueue.isEmpty() && MessageBuffer.getInstance().isFull()) {}

        System.out.println("Buffer event");
        if(MessageBuffer.getInstance().isFull())
            reply();
    }
}