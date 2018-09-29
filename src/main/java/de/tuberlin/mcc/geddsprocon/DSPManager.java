package de.tuberlin.mcc.geddsprocon;


import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.IDSPConnector;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.SocketPool;
import de.tuberlin.mcc.geddsprocon.messagebuffer.IMessageBufferFunction;
import de.tuberlin.mcc.geddsprocon.messagebuffer.IMessageBufferListener;
import de.tuberlin.mcc.geddsprocon.messagebuffer.MessageBuffer;
import de.tuberlin.mcc.geddsprocon.tuple.Tuple2;
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
            if(ready.equals("MASTER")) {
                System.out.println("MASTER received");
                if(!reply(address))
                    reply();
            } else if(ready.equals("READY")) {
                System.out.println("READY received");
                if(this.endpointSet.add(address))
                    this.endpointQueue.add(address);
            }
        }
    }

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

    private boolean reply(ZFrame address) {
        ZMsg message = new ZMsg();
        message.add(address);
        message.add("");
        System.out.println("Before Loop ");
        while(MessageBuffer.getInstance().isEmpty()) {/*System.out.println("Buffer empty" + MessageBuffer.getInstance().getMessages());*/}
        System.out.println("After Loop ");
        message.append(MessageBuffer.getInstance().flushBuffer(this.bufferFunction));
        System.out.println("Sending message " + message.toString());
        return message.send(this.socket);
    }

    @Override
    public void bufferIsFullEvent() {
        System.out.println("Buffer event");
        while(this.endpointQueue.isEmpty() && MessageBuffer.getInstance().isFull()) {}

        if(MessageBuffer.getInstance().isFull())
            reply();
    }
}