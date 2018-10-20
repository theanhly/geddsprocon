package de.tuberlin.mcc.geddsprocon.messagebuffer;

import com.google.common.base.Strings;
import de.tuberlin.mcc.geddsprocon.DSPConnectorConfig;
import de.tuberlin.mcc.geddsprocon.common.JavaProcessBuilder;
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
    private volatile int messages = 0;
    private LinkedList<IMessageBufferListener> listener;
    private ZMQ.Socket bufferSocket;
    private final ZMQ.Context context = ZMQ.context(1);
    private boolean init;

    public MessageBuffer() {
        this.listener = new LinkedList<>();

        this.init = false;
        this.bufferSocket = this.context.socket(ZMQ.REQ);
        this.bufferSocket.setReceiveTimeOut(5000);
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

        String connectionString;
        for(int i = 0; true; i++) {
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
        }

        return connectionString;
    }

    /**
     * Writes bytes to the buffer process.
     * @param bytes bytes which are written into the buffer
     */
    public void writeBuffer(byte[] bytes) {
        // buffer gets overwritten if buffer isn't flushed in time
        synchronized (this.bufferLock) {
            //this.buffer[this.messages%this.bufferSize] = bytes;
            ZMsg writeMessage = new ZMsg();
            writeMessage.add(this.WRITE_MESSAGE);
            writeMessage.add(bytes);
            writeMessage.send(this.bufferSocket);

            assert(this.bufferSocket.recvStr().equals("WRITE_SUCCESS"));

            this.messages++;
        }
        if(isFull())
        {
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
            if(previousBuffer)
                this.bufferSocket.send(this.PEEKPREVBUFFER_MESSAGE);
            else
                this.bufferSocket.send(this.PEEKBUFFER_MESSAGE);

            ZMsg messages = bufferFunction.flush(ZMsg.recvMsg(this.bufferSocket));

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
            this.bufferSocket.send(this.CLEARBUFFER_MESSAGE);
            assert(this.bufferSocket.recvStr().equals("CLEAR_SUCCESS"));
            this.messages = 0;
            //Arrays.fill(this.buffer, new byte[]{(byte)0});
        }
    }

    /**
     * method for testing purposes
     * @return number of messages in the buffer
     */
    public int getMessages() {
        return this.messages;
    }

    /**
     * method for testing purposes
     * @return number of messages in the buffer
     */
    public long getSentMessages() {
        synchronized (this.bufferLock) {
            this.bufferSocket.send(this.SENTMESSAGES_MESSAGE);
            return Long.parseLong(this.bufferSocket.recvStr());
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
