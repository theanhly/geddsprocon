package de.tuberlin.mcc.geddsprocon.messagebuffer;

import org.zeromq.ZMsg;

public interface IMessageBufferFunction {

    ZMsg flush(ZMsg message);
}
