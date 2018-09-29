package de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors;


import de.tuberlin.mcc.geddsprocon.DSPConnectorConfig;
import org.junit.Assert;
import org.junit.Test;
import org.zeromq.ZMQ;

public class SocketPoolTest {

    @Test
    public void getSocketTest() {
        try {
            ZMQ.Socket socket = SocketPool.getInstance().getOrCreateSocket(SocketPool.SocketType.PUSH, "localhost", 5555, new DSPConnectorConfig.Builder("localhost", 5555).build());
            ZMQ.Socket socket2 = SocketPool.getInstance().getOrCreateSocket("localhost", 5555);
            ZMQ.Socket socket3 = SocketPool.getInstance().getOrCreateSocket(SocketPool.SocketType.PUSH,"localhost", 5556, new DSPConnectorConfig.Builder("localhost", 5555).build());
            Assert.assertEquals(socket, socket);
            Assert.assertNotEquals(socket, socket3);
            Assert.assertNotEquals(socket2, socket3);
        } catch(IllegalArgumentException ex) {
            System.out.println(ex.toString());
        }
    }
}
