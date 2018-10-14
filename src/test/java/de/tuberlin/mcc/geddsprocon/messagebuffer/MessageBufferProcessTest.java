package de.tuberlin.mcc.geddsprocon.messagebuffer;

import de.tuberlin.mcc.geddsprocon.common.JavaProcessBuilder;
import org.junit.Test;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

public class MessageBufferProcessTest {

    @Test
    public void testInit() throws IOException, InterruptedException{
        Process process = JavaProcessBuilder.exec(MessageBufferProcess.class);
        System.out.println("Thread ID " + Thread.currentThread().getId() + ": Test ending.");
        ZMQ.Context context = ZMQ.context(1);

        //  Socket to talk to server
        System.out.println("Thread ID " + Thread.currentThread().getId() + ": Connecting to hello world server…");

        ZMQ.Socket requester = context.socket(ZMQ.REQ);
        requester.setReceiveTimeOut(10000);
        requester.connect("ipc:///message-buffer-process");

        requester.send("INIT");

        //byte[] test = requester.recv(0);
        //System.out.println(SerializationUtils.deserialize(test));
        //if(reply != null)
        System.out.println("Thread ID " + Thread.currentThread().getId() + ": Trying to receive");

        ZMsg messages = ZMsg.recvMsg(requester);

        for(ZFrame frame : messages) {
            System.out.println(frame.toString());
            //System.out.println(new String(frame.getData()));
            Thread.sleep(100);
        }

        requester.send("END");

        System.out.println("Thread ID " + Thread.currentThread().getId() + ": Trying to receive");

        messages = ZMsg.recvMsg(requester);

        for(ZFrame frame : messages) {
            System.out.println(frame.toString());
            //System.out.println(new String(frame.getData()));
            Thread.sleep(100);
        }

        process.waitFor();
    }

    @Test
    public void writeBufferTest() throws IOException, InterruptedException{
        Process process = JavaProcessBuilder.exec(MessageBufferProcess.class);
        System.out.println("Thread ID " + Thread.currentThread().getId() + ": Test ending.");
        ZMQ.Context context = ZMQ.context(1);

        //  Socket to talk to server
        System.out.println("Thread ID " + Thread.currentThread().getId() + ": Connecting to hello world server…");

        ZMQ.Socket requester = context.socket(ZMQ.REQ);
        requester.setReceiveTimeOut(10000);
        requester.connect("ipc:///message-buffer-process");

        // Write buffer
        ZMsg message = new ZMsg();

        message.add("WRITE");
        message.add("HALLO TEST".getBytes());

        message.send(requester);

        System.out.println("Thread ID " + Thread.currentThread().getId() + ": ’" + requester.recvStr() +  "’ received ");

        message = new ZMsg();

        message.add("WRITE");
        message.add("HALLO TEST2".getBytes());

        message.send(requester);

        System.out.println("Thread ID " + Thread.currentThread().getId() + ": ’" + requester.recvStr() +  "’ received ");

        // Peek buffer
        requester.send("PEEKBUFFER");

        System.out.println("Thread ID " + Thread.currentThread().getId() + ": Trying to peek buffer #1.");

        ZMsg messages = ZMsg.recvMsg(requester);

        for(ZFrame frame : messages) {
            System.out.println("frame received [" + frame.toString() + "]");
            //System.out.println(new String(frame.getData()));
            Thread.sleep(100);
        }

        // Peek buffer again
        requester.send("PEEKBUFFER");

        System.out.println("Thread ID " + Thread.currentThread().getId() + ": Trying to peek buffer #2.");

        messages = ZMsg.recvMsg(requester);

        for(ZFrame frame : messages) {
            System.out.println("frame received [" + frame.toString() + "]");
            //System.out.println(new String(frame.getData()));
            Thread.sleep(100);
        }

        // Clear buffer
        requester.send("CLEARBUFFER");

        System.out.println("Thread ID " + Thread.currentThread().getId() + ": Trying to clear buffer.");

        messages = ZMsg.recvMsg(requester);

        for(ZFrame frame : messages) {
            System.out.println("frame received [" + frame.toString() + "]");
            //System.out.println(new String(frame.getData()));
            Thread.sleep(100);
        }

        // Peek buffer after clearing
        requester.send("CLEARBUFFER");

        System.out.println("Thread ID " + Thread.currentThread().getId() + ": Trying to peek buffer #3.");

        messages = ZMsg.recvMsg(requester);

        for(ZFrame frame : messages) {
            System.out.println("frame received [" + frame.toString() + "]");
            //System.out.println(new String(frame.getData()));
            Thread.sleep(100);
        }

        // End message buffer process
        requester.send("END");

        System.out.println("Thread ID " + Thread.currentThread().getId() + ": Trying to receive");

        messages = ZMsg.recvMsg(requester);

        for(ZFrame frame : messages) {
            System.out.println(frame.toString());
            //System.out.println(new String(frame.getData()));
            Thread.sleep(100);
        }

        process.waitFor();
    }
}
