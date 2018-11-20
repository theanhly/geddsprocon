package de.tuberlin.mcc.geddsprocon.geddsproconcore.messagebuffer;

import com.google.common.base.Strings;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.DSPConnectorConfig;
import de.tuberlin.mcc.geddsprocon.geddsproconmessagebuffer.JavaProcessBuilder;
import de.tuberlin.mcc.geddsprocon.geddsproconmessagebuffer.MessageBufferProcess;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.util.LinkedList;

public class MessageBuffer {

    // doesn't define the upper bound of maximum messages. describes the number the buffer has to reset
    public static final long RESETMESSAGENUMBER           = 9000000000000000000L;
    public static final String INIT_MESSAGE             = "INIT";
    public static final String WRITE_MESSAGE            = "WRITE";
    public static final String PEEKBUFFER_MESSAGE       = "PEEKBUFFER";
    public static final String PEEKPREVBUFFER_MESSAGE   = "PEEKPREVBUFFER";
    public static final String MESSAGECOUNT_MESSAGE     = "MESSAGECOUNT";
    public static final String SENTMESSAGES_MESSAGE     = "SENTMESSAGES";
    public static final String CLEARBUFFER_MESSAGE      = "CLEARBUFFER";
    public static final String END_MESSAGE              = "END";

    private final Object bufferLock = new Object();
    private int bufferSize = 1000;
    private byte[][] buffer;
    private volatile int messages;
    private LinkedList<IMessageBufferListener> listener;
    private ZMQ.Socket bufferSocket;
    private final ZMQ.Context context = ZMQ.context(1);
    private boolean init;
    private ZMsg messageBuffer;
    private ZMsg previousMessageBuffer;
    private boolean addSentMessagesFrame;
    private volatile long sentMessagesID;

    public MessageBuffer() {
        this.listener = new LinkedList<>();

        this.init = false;
        this.bufferSocket = this.context.socket(ZMQ.REQ);
        this.bufferSocket.setReceiveTimeOut(5000);
        messages = 0;
    }

    public void addListener(IMessageBufferListener listener) {
        this.listener.add(listener);
    }

    public byte[][] getBuffer()
    {
        return this.buffer;
    }

    public boolean isFull() {
        return this.messages >= this.bufferSize;
    }

    public boolean isEmpty() {
        return this.messages <= 0;
    }

    /**
     * Initiate the buffer with the default values
     */
    public String initiateBuffer(DSPConnectorConfig config) {
        return initiateBuffer(config, true);
    }

    /**
     * Initiate the buffer with a config and determine if a message id frame should be added.
     * @param config initiate buffer and buffer process with config
     * @param addSentMessagesFrame determines if a message id frame should be added. Unnecessary if the buffer is in an input operator.
     */
    public String initiateBuffer(DSPConnectorConfig config, boolean addSentMessagesFrame) {
        if(config != null) {
            this.bufferSize = config.getHwm();

        }

        this.addSentMessagesFrame = addSentMessagesFrame;

        String connectionString = "ipc:///" + config.getBufferConnectionString();

        this.sentMessagesID = 1;
        this.messageBuffer = new ZMsg();
        if(this.addSentMessagesFrame)
            this.messageBuffer.add(Long.toString(sentMessagesID));

        /*for(int i = 0; true; i++) {
            connectionString = Strings.isNullOrEmpty(config.getBufferConnectionString()) ? "ipc:///message-buffer-process-" + i : "ipc:///" + config.getBufferConnectionString();
            this.bufferSocket.connect(connectionString);
            this.bufferSocket.send(this.INIT_MESSAGE);
            String reply = this.bufferSocket.recvStr();
            if(Strings.isNullOrEmpty(reply)) {
                try {
                    JavaProcessBuilder.exec(MessageBufferProcess.class, connectionString, addSentMessagesFrame);
                    reply = this.bufferSocket.recvStr();
                    System.out.println(reply);
                    assert(reply.equals("OK"));
                    break;
                } catch(Exception ex) {
                    System.err.println("Starting message buffer process failed.");
                    System.err.println(ex.toString());
                }
            } else {
                if(Strings.isNullOrEmpty(config.getBufferConnectionString()))
                    this.bufferSocket.disconnect(connectionString);
                else {
                    assert(reply.equals("OK"));
                    this.messages = 0;
                    this.bufferSocket.send(this.MESSAGECOUNT_MESSAGE);
                    this.messages = Integer.parseInt(this.bufferSocket.recvStr());

                    System.out.println("Old MessageBuffer found. Message count: " + this.messages);
                    break;
                }
            }
        }*/


        System.out.println("Init buffer with connection string: " + connectionString + " @Thread-ID: " + Thread.currentThread().getId());
        return connectionString;
    }

    /**
     * Writes bytes to the buffer process.
     * @param bytes bytes which are written into the buffer
     */
    public void writeBuffer(byte[] bytes) {
        // buffer gets overwritten if buffer isn't flushed in time
        synchronized (this.bufferLock) {
            /*//this.buffer[this.messages%this.bufferSize] = bytes;
            //System.out.println("===== Message buffer writing start.");
            ZMsg writeMessage = new ZMsg();
            writeMessage.add(this.WRITE_MESSAGE);
            writeMessage.add(bytes);
            writeMessage.send(this.bufferSocket);

            // do not receive the response in assert since it seems to be non blocking -> results in exception if run in a cluster
            String response = this.bufferSocket.recvStr();
            assert(response.equals("WRITE_SUCCESS"));*/
            //System.out.println("===== Message buffer writing start.");
            this.messageBuffer.add(bytes);

            this.messages++;
            //System.out.println("===== Message buffer writing end.");

            /* OLD
            //this.buffer[this.messages%this.bufferSize] = bytes;
            //System.out.println("===== Message buffer writing start.");
            ZMsg writeMessage = new ZMsg();
            writeMessage.add(this.WRITE_MESSAGE);
            writeMessage.add(bytes);
            writeMessage.send(this.bufferSocket);

            // do not receive the response in assert since it seems to be non blocking -> results in exception if run in a cluster
            String response = this.bufferSocket.recvStr();
            assert(response.equals("WRITE_SUCCESS"));

            this.messages++;
            //System.out.println("===== Message buffer writing end.");
             */
        }
        if(isFull())
        {
            System.out.println("Buffer full");
            // tell all the listeners that the buffer is full
            for (IMessageBufferListener listener : this.listener ) {
                listener.bufferIsFullEvent();
            }
        }
    }

    /**
     * flush buffer. requires a buffer function to determine what to do with the buffer
     * @param bufferFunction
     * @return ZeroMQ multi part message
     */
    public ZMsg flushBuffer(IMessageBufferFunction bufferFunction) {
        synchronized(this.bufferLock) {
            return flushBuffer(bufferFunction, true);
        }
    }

    /**
     * flush buffer. requires a buffer function to determine what to do with the buffer
     * @param bufferFunction buffer function which determines what to do with the buffer
     * @param clearBuffer clearing the buffer
     * @return ZeroMQ multi part message
     */
    public ZMsg flushBuffer(IMessageBufferFunction bufferFunction, boolean clearBuffer) {
        synchronized(this.bufferLock) {
            return flushBuffer(bufferFunction, clearBuffer, false);
        }
    }

    /**
     * Flush the buffer. In case the previous buffer is flushed the current buffer is not cleared.
     * @param bufferFunction buffer function which determines what to do with the buffer
     * @param clearBuffer clearing the buffer
     * @param previousBuffer Flah in case the previous buffer is required
     * @return the buffer in form of a ZMsg
     */
    public ZMsg flushBuffer(IMessageBufferFunction bufferFunction, boolean clearBuffer, boolean previousBuffer) {
        synchronized(this.bufferLock) {
            // callback call flushing of buffer
            ZMsg messages;
            if(previousBuffer) {
                //this.bufferSocket.send(this.PEEKPREVBUFFER_MESSAGE);
                messages = this.previousMessageBuffer.duplicate();
            }
            else {
                // this.bufferSocket.send(this.PEEKBUFFER_MESSAGE);
                messages = this.messageBuffer.duplicate();
            }

            //ZMsg messages = bufferFunction.flush(ZMsg.recvMsg(this.bufferSocket));
            messages = bufferFunction.flush(messages);

            if(clearBuffer)
                clearBuffer();

            return messages;
        }
    }

    /**
     * Clear the buffer
     */
    public void clearBuffer() {
        synchronized(this.bufferLock) {
            /*this.bufferSocket.send(this.CLEARBUFFER_MESSAGE);

            //
            String response = this.bufferSocket.recvStr();
            assert(response.equals("CLEAR_SUCCESS"));*/
            if(this.previousMessageBuffer != null)
                this.previousMessageBuffer.destroy();

            this.previousMessageBuffer = this.messageBuffer.duplicate();
            messageBuffer.destroy();
            if(this.addSentMessagesFrame)
                messageBuffer.add(Long.toString(++this.sentMessagesID));

            this.messages = 0;

            /* OLD
            String response = this.bufferSocket.recvStr();
            assert(response.equals("CLEAR_SUCCESS"));
            this.messages = 0;
             */
        }
    }

    /**
     * method for testing purposes
     * @return number of messages in the buffer
     */
    public int getMessages() {
        return this.messages;
    }


    public long getSentMessages() {
        synchronized (this.bufferLock) {
            /* OLD
            this.bufferSocket.send(this.SENTMESSAGES_MESSAGE);
            return Long.parseLong(this.bufferSocket.recvStr());
             */
            return this.sentMessagesID;
        }
    }

    /**
     * Get the a lock for the message buffer
     * @return buffer lock
     */
    public Object getBufferLock() {
        return this.bufferLock;
    }
}
