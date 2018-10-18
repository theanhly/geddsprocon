package de.tuberlin.mcc.geddsprocon.messagebuffer;

import com.google.common.base.Strings;
import de.tuberlin.mcc.geddsprocon.DSPConnectorConfig;
import de.tuberlin.mcc.geddsprocon.common.JavaProcessBuilder;
import de.tuberlin.mcc.geddsprocon.tuple.Tuple2;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.util.Arrays;
import java.util.LinkedList;

public class MessageBuffer {

    public static final String INIT_MESSAGE         = "INIT";
    public static final String WRITE_MESSAGE        = "WRITE";
    public static final String PEEKBUFFER_MESSAGE   = "PEEKBUFFER";
    public static final String MESSAGECOUNT_MESSAGE = "MESSAGECOUNT";
    public static final String CLEARBUFFER_MESSAGE  = "CLEARBUFFER";
    public static final String END_MESSAGE          = "END";

    private static MessageBuffer ourInstance = new MessageBuffer();

    public static MessageBuffer getInstance() {
        return ourInstance;
    }

    private final Object bufferLock = new Object();
    private int bufferSize = 1000;
    private byte[][] buffer;
    private volatile int messages = 0;
    private LinkedList<IMessageBufferListener> listener;
    private ZMQ.Socket bufferSocket;
    private final ZMQ.Context context = ZMQ.context(1);
    private DSPConnectorConfig config;
    private boolean init;
    //private final String connectionString;

    private MessageBuffer() {
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
     * initiate the buffer with the default size
     */
    public void initiateBuffer() {
        initiateBuffer(null);
    }

    /**
     * initiate the buffer. the buffer size determines the messages the buffer should hold
     * @param config initiate buffer and buffer process with config
     */
    public void initiateBuffer(DSPConnectorConfig config) {
        if(config != null) {
            this.bufferSize = config.getHwm();
            //this.buffer = new byte[this.bufferSize][];

        }


        String connectionString;
        for(int i = 0; true; i++) {
            connectionString = Strings.isNullOrEmpty(config.getBufferConnectionString()) ? "ipc:///message-buffer-process-" + i : "ipc:///" + config.getBufferConnectionString();
            this.bufferSocket.connect(connectionString);
            this.bufferSocket.send(this.INIT_MESSAGE);
            String reply = this.bufferSocket.recvStr();
            if(Strings.isNullOrEmpty(reply)) {
                try {
                    JavaProcessBuilder.exec(MessageBufferProcess.class, connectionString);
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
                    //ZMsg messagesInBuffer = ZMsg.recvMsg(this.bufferSocket, ZMQ.DONTWAIT);
                    //if(messagesInBuffer != null)
                    //    this.messages = messagesInBuffer.toArray().length;
                    this.messages = Integer.parseInt(this.bufferSocket.recvStr());

                    System.out.println("Old MessageBuffer found. Message count: " + this.messages);
                    break;
                }
            }
        }
        /*
        synchronized (this.bufferLock) {
            if(!init) {
                String connectionString;
                for(int i = 0; true; i++) {
                    connectionString = Strings.isNullOrEmpty(config.getBufferConnectionString()) ? "ipc:///message-buffer-process-" + i : "ipc:///" + config.getBufferConnectionString();
                    this.bufferSocket.connect(connectionString);
                    this.bufferSocket.send(this.INIT_MESSAGE);
                    String reply = this.bufferSocket.recvStr();
                    if(Strings.isNullOrEmpty(reply)) {
                        try {
                            JavaProcessBuilder.exec(MessageBufferProcess.class, connectionString);
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
                            //ZMsg messagesInBuffer = ZMsg.recvMsg(this.bufferSocket, ZMQ.DONTWAIT);
                            //if(messagesInBuffer != null)
                            //    this.messages = messagesInBuffer.toArray().length;
                            this.messages = Integer.parseInt(this.bufferSocket.recvStr());

                            System.out.println("Old MessageBuffer found. Message count: " + this.messages);
                            break;
                        }
                    }
                }

                init = true;
            }
        }*/
    }

    /**
     * writes bytes to the buffer. if the messages surpass the buffer size old messages are overwritten
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
     * @param bufferFunction
     * @param clearBuffer clearing the buffer
     * @return ZeroMQ multi part message
     */
    public ZMsg flushBuffer(IMessageBufferFunction bufferFunction, boolean clearBuffer) {
        synchronized(this.bufferLock) {
            //if(!isFull()) {
                //writeBuffer(new byte[] {(byte)0});
            //}


            // callback call flushing of buffer
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

    public Object getBufferLock() {
        return this.bufferLock;
    }
}
