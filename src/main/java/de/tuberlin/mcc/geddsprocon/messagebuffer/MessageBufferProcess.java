package de.tuberlin.mcc.geddsprocon.messagebuffer;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

public class MessageBufferProcess {

    private static final int defaultBufferSize = 1000;

    public static void main(String[] args) {
        String connectionString = "ipc:///message-buffer-process";
        boolean addSentMessagesFrame = true;
        if(args.length > 0)
            connectionString = args[0];

        if(args.length > 1)
            addSentMessagesFrame = Boolean.parseBoolean(args[1]);

        System.out.println("MessageBufferProcess started @" + connectionString);
        try (ZContext context = new ZContext()) {
            // Socket to talk to clients
            ZMQ.Socket socket = context.createSocket(ZMQ.REP);
            socket.bind(connectionString);
            long sentMessages = 1;
            ZMsg message = new ZMsg();
            if(addSentMessagesFrame)
                message.add(Long.toString(sentMessages));

            ZMsg previousMessage = null;

            while(true) {
                // Block until a message is received
                ZMsg receivedMessage = ZMsg.recvMsg(socket);

                String command = receivedMessage.pop().toString();

                // Reset the number to 0 in case the reset message number has been reached. necessary because we cannot simply count indefinitely
                if(sentMessages == MessageBuffer.RESETMESSAGENUMBER)
                    sentMessages = 0;

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
                    if(addSentMessagesFrame)
                        message.add(Long.toString(++sentMessages));
                    socket.send("CLEAR_SUCCESS");
                    System.out.println("CLEARBUFFER end.");
                } else if(command.equals("PEEKPREVBUFFER")) {
                    if(previousMessage != null)
                        previousMessage.duplicate().send(socket, false);
                    else
                        message.duplicate().send(socket, false);

                    System.out.println("PEEKPREVBUFFER end.");
                } else if(command.equals("END")) {
                    System.out.println("END received");
                    socket.send("END_SUCCESS");
                    break;
                } else if (command.equals("MESSAGECOUNT")) {
                    System.out.println("MESSAGECOUNT received");
                    socket.send(Integer.toString(message.toArray().length));
                } else if (command.equals("SENTMESSAGES")) {
                    System.out.println("SENTMESSAGES received");
                    socket.send(Long.toString(sentMessages));
                } else {
                    System.out.println("Unknown command received: " + command);
                    socket.send(command);
                }

                if(previousMessage != null)
                    assert(Integer.parseInt(previousMessage.peek().toString()) + 1 == Integer.parseInt(message.peek().toString()));
            }
        }

        System.out.println("Thread ID " + Thread.currentThread().getId() + ": MessageBufferProcess ended.");

    }
}
