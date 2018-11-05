package de.tuberlin.mcc.geddsprocon.geddsproconcore.datastreamprocessorconnectors;

import de.tuberlin.mcc.geddsprocon.geddsproconcore.messagebuffer.IMessageBufferFunction;

public interface IDSPSinkConnector {

    /**
     * return the buffer function which determines what to do with the byte message. Writing it
     * @return buffer function
     */
    IMessageBufferFunction getBufferFunction();
}
