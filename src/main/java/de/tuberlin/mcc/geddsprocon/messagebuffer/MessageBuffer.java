package de.tuberlin.mcc.geddsprocon.messagebuffer;

import de.tuberlin.mcc.geddsprocon.tuple.Tuple2;
import org.zeromq.ZMsg;

import java.util.LinkedList;

public class MessageBuffer {
    private static MessageBuffer ourInstance = new MessageBuffer();

    public static MessageBuffer getInstance() {
        return ourInstance;
    }
    private int bufferSize = 1000;
    private byte[][] buffer;
    private volatile int messages = 0;
    private final Object bufferLock = new Object();
    private LinkedList<IMessageBufferListener> listener;

    private MessageBuffer() {
        this.listener = new LinkedList<>();
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
        initiateBuffer(this.bufferSize);
    }

    /**
     * initiate the buffer. the buffer size determines the messages the buffer should hold
     * @param bufferSize
     */
    public void initiateBuffer(int bufferSize) {
        this.bufferSize = bufferSize;
        this.buffer = new byte[this.bufferSize][];
    }

    /**
     * writes bytes to the buffer. if the messages surpass the buffer size old messages are overwritten
     * @param bytes bytes which are written into the buffer
     */
    public void writeBuffer(byte[] bytes) {
        // buffer gets overwritten if buffer isn't flushed in time
        synchronized (this.bufferLock) {
            this.buffer[this.messages%this.bufferSize] = bytes;
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
     * @return
     */
    public ZMsg flushBuffer(IMessageBufferFunction bufferFunction) {
        synchronized(this.bufferLock) {
            if(!isFull())
                writeBuffer(new byte[] {(byte)0});

            // callback call flushing of buffer
            ZMsg messages = bufferFunction.flush(this);
            this.messages = 0;
            return messages;
        }
    }

    /**
     * method for testing purposes
     * @return
     */
    public int getMessages() {
        return this.messages;
    }
}
