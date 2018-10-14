package de.tuberlin.mcc.geddsprocon.messagebuffer;

import io.netty.buffer.ByteBuf;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;

public class MessageBufferProcess {

    private static final int defaultBufferSize = 1000;

    public static void main(String[] args) {
        String connectionString = "ipc:///message-buffer-process";
        if(args.length > 0)
            connectionString = args[0];

        System.out.println("Thread ID " + Thread.currentThread().getId() + ": MessageBufferProcess started @" + connectionString);
        try (ZContext context = new ZContext()) {
            // Socket to talk to clients
            ZMQ.Socket socket = context.createSocket(ZMQ.REP);
            socket.bind(connectionString);

            ZMsg message = new ZMsg();
            //int flushCount = 0;
            //message.add(Integer.toString(flushCount));
            ZMsg previousMessage = new ZMsg();

            System.out.println("Thread ID " + Thread.currentThread().getId() + ": Starting while loop");
            while(true) {
                // Block until a message is received
                ZMsg receivedMessage = ZMsg.recvMsg(socket);

                String command = receivedMessage.pop().toString();

                if(command.equals("INIT")) {
                    System.out.println("INIT received");
                    socket.send("OK");
                } else if(command.equals("WRITE")) {
                    byte[] data = receivedMessage.pop().getData();
                    message.add(data);
                    socket.send("WRITE_SUCCESS");
                    System.out.println("WRITE end.");
                } else if(command.equals("PEEKBUFFER")) {
                    message.send(socket, false);
                    System.out.println("PEEKBUFFER end.");
                } else if(command.equals("CLEARBUFFER")) {
                    if(previousMessage != null)
                        previousMessage.destroy();

                    previousMessage = message.duplicate();
                    message.destroy();
                    socket.send("CLEAR_SUCCESS");
                    System.out.println("CLEARBUFFER end.");
                } else if(command.equals("END")) {
                    System.out.println("END received");
                    socket.send("END_SUCCESS");
                    break;
                } else {
                    socket.send(command);
                }
            }

            /*if(reply != null ) {
                System.out.println(
                        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date()).toString() + " Thread ID " + Thread.currentThread().getId() + ": Received " + ": [" + reply + "]"
                );
            }*/

            //socket.send("Thread ID " + Thread.currentThread().getId() + ": " +  reply + " received");
        }

        System.out.println("Thread ID " + Thread.currentThread().getId() + ": MessageBufferProcess ended.");

    }
}
