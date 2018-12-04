package de.tuberlin.mcc.geddsprocon.geddsproconcore.messagebuffer;

public interface IMessageBufferListener {
    /**
     * Event to signal that the buffer is full
     */
    void bufferIsFullEvent();

    /**
     * Event to signal that the buffer has been cleared
     */
    void bufferClearedEvent();
}
