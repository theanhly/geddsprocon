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

public class DSPManager implements Runnable, IMessageBufferListener {

    private String host;
    private int port;
    private volatile IMessageBufferFunction bufferFunction;
    private volatile LinkedList<ZFrame> endpointQueue;
    private volatile HashSet<ZFrame> endpointSet;
    private final ZMQ.Socket socket;

    public DSPManager(String host, int port, IMessageBufferFunction bufferFunction) {
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

            //  Second frame is empty
            String empty = new String(msg.pop().getData());
            assert (empty.length() == 0);

            String ready = new String(msg.pop().getData());

            if(ready.equals(DSPConnectorFactory.ConnectorType.PRIMARY)) {
                System.out.println(DSPConnectorFactory.ConnectorType.PRIMARY + " message received");
                if(!reply(address))
                    reply();
            } else if(ready.equals(DSPConnectorFactory.ConnectorType.SECONDARY)) {
                System.out.println(DSPConnectorFactory.ConnectorType.SECONDARY +" message received");
                if(this.endpointSet.add(address))
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
        message.add("");
        while(MessageBuffer.getInstance().isEmpty()) {}
        message.append(MessageBuffer.getInstance().flushBuffer(this.bufferFunction));
        System.out.println("Sending message " + message.toString());
        return message.send(this.socket);
    }

    /**
     * is used when the buffer is full. go through the queue and find an address to reply to
     */
    @Override
    public void bufferIsFullEvent() {
        System.out.println("Buffer event");
        while(this.endpointQueue.isEmpty() && MessageBuffer.getInstance().isFull()) {}

        if(MessageBuffer.getInstance().isFull())
            reply();
    }
}