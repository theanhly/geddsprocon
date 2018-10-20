package de.tuberlin.mcc.geddsprocon.messagebuffer;

public interface IMessageBufferListener {
    /**
     * Event to signal that the buffer is full
     */
    void bufferIsFullEvent();
}
