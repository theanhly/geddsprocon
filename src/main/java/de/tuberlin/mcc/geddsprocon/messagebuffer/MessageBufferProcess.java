package de.tuberlin.mcc.geddsprocon.messagebuffer;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.text.SimpleDateFormat;
import java.util.Date;

public class MessageBufferProcess {

    public static void main(String[] args) throws Exception{
        System.out.println("Thread ID " + Thread.currentThread().getId() + ": MessageBufferProcess started.");
        if(args.length > 0)
            System.out.println("Thread ID " + Thread.currentThread().getId() + ": Received argument: " + args[0]);
        Thread.sleep(4000);

        try (ZContext context = new ZContext()) {
            // Socket to talk to clients
            ZMQ.Socket socket = context.createSocket(ZMQ.REP);
            socket.setReceiveBufferSize(1);
            socket.setHWM(1);
            //socket.setReceiveBufferSize(5);
            socket.bind("ipc:///test");

            System.out.println("Thread ID " + Thread.currentThread().getId() + ": Starting while loop");
            // Block until a message is received
            String reply = socket.recvStr();

            if(reply != null ) {
                System.out.println(
                        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date()).toString() + " Thread ID " + Thread.currentThread().getId() + ": Received " + ": [" + reply + "]"
                );
            }

            socket.send("Thread ID " + Thread.currentThread().getId() + ": " +  reply + " received");
        }

        System.out.println("Thread ID " + Thread.currentThread().getId() + ": MessageBufferProcess ended.");

    }
}
