package de.tuberlin.mcc.geddsprocon.geddsproconcore.messagebuffer;

import org.zeromq.ZMsg;

public interface IMessageBufferFunction {

    /**
     * Return the buffer function which determines how the buffer should be flushed
     * @return
     */
    ZMsg flush(ZMsg message);
}
