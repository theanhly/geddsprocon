package de.tuberlin.mcc.geddsprocon.geddsproconcore;

import com.google.common.base.Strings;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.datastreamprocessorconnectors.SocketPool;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.messagebuffer.IMessageBufferFunction;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.messagebuffer.IMessageBufferListener;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.messagebuffer.MessageBuffer;
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
    private boolean resendPrevBuffer;
    private int lastReceivedMessageNumber;
    private String messageBufferConnectionString;

    public DSPRouter(String host, int port, IMessageBufferFunction bufferFunction, String messageBufferConnectionString) {
        this.host = host;
        this.port = port;
        this.bufferFunction = bufferFunction;
        this.messageBufferConnectionString = messageBufferConnectionString;
        this.endpointQueue = new LinkedList<>();
        this.endpointSet = new HashSet<>();
        this.socket = SocketPool.getInstance().getOrCreateSocket(this.host, this.port);
        this.resendPrevBuffer = false;
        this.lastReceivedMessageNumber = -1;

        if(messageBufferConnectionString == null || messageBufferConnectionString.isEmpty())
            System.err.println("Buffer string cannot be null or empty");
    }

    @Override
    public void run() {


        while(true) {
            //System.out.println("Trying to receive");
            ZMsg msg = ZMsg.recvMsg(this.socket);

            if(msg != null && !msg.isEmpty()) {
                //  First frame is address
                ZFrame address = msg.pop();

                //  Second frame is empty in a REQ socket. Second frame of DEALER socket is not empty
                String empty = new String(msg.pop().getData());
                assert (empty.length() == 0);

                String ready = new String(msg.pop().getData());
                assert(ready.length() > 0);

                this.lastReceivedMessageNumber = Math.max(Integer.parseInt(msg.pop().toString()), this.lastReceivedMessageNumber);

                if(ready.equals(DSPConnectorFactory.ConnectorType.PRIMARY)) {
                    //System.out.println(DSPConnectorFactory.ConnectorType.PRIMARY + " message received");

                    if(reply(address)) {
                        this.temporaryPrimary = address;
                    } else {
                        System.out.println("Message could not be sent.");
                    }
                } else if(ready.equals(DSPConnectorFactory.ConnectorType.SECONDARY)) {
                    //System.out.println(DSPConnectorFactory.ConnectorType.SECONDARY +" message received");

                    if(this.temporaryPrimary != null && this.temporaryPrimary.hasData() && this.temporaryPrimary.getData().equals(address.getData()))
                        reply(address);
                    else if(DSPManager.getInstance().getBuffer(this.messageBufferConnectionString).isFull()) {
                        this.temporaryPrimary = address.duplicate();
                        reply(address);
                    }
                    else if(this.endpointSet.add(address))
                        this.endpointQueue.add(address);
                }
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
            //System.out.println("Address popped");
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
        message.add("");

        boolean resendPrevBuffer = false;

        long currentMessages = DSPManager.getInstance().getBuffer(this.messageBufferConnectionString).getSentMessages();

        if(currentMessages > 1 && (this.lastReceivedMessageNumber + 1 != currentMessages || (this.lastReceivedMessageNumber == MessageBuffer.RESETMESSAGENUMBER && currentMessages != 1))) {
            resendPrevBuffer = true;
        }

        if(!resendPrevBuffer && DSPManager.getInstance().getBuffer(this.messageBufferConnectionString).isEmpty()) {
            message.add("");
            return message.send(this.socket);
        }

        Object bufferLock = DSPManager.getInstance().getBuffer(this.messageBufferConnectionString).getBufferLock();
        synchronized (bufferLock) {
            if(resendPrevBuffer)
                System.out.println("Resending buffer");
            else
                System.out.println("Sending new  buffer");
            message.append(DSPManager.getInstance().getBuffer(this.messageBufferConnectionString).flushBuffer(this.bufferFunction, false, resendPrevBuffer));
            //System.out.println("Sending message with message-id: " + message.peek().toString());
            if(message.send(this.socket)) {
                // only clear buffer after sending has been successful and the previous buffer wasn't sent
                if(!resendPrevBuffer)
                    DSPManager.getInstance().getBuffer(this.messageBufferConnectionString).clearBuffer();

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
        while(this.endpointQueue.isEmpty() && DSPManager.getInstance().getBuffer(this.messageBufferConnectionString).isFull()) {}

        //this.resendPrevBuffer = true;
        //System.out.println("Buffer event");
        if(DSPManager.getInstance().getBuffer(this.messageBufferConnectionString).isFull())
            reply();
    }
}