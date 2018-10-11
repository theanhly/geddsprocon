package de.tuberlin.mcc.geddsprocon.messagebuffer;

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
        Process process = JavaProcess.exec(MessageBufferProcess.class);
        System.out.println("Thread ID " + Thread.currentThread().getId() + ": Test ending.");
        ZMQ.Context context = ZMQ.context(1);

        //  Socket to talk to server
        System.out.println("Thread ID " + Thread.currentThread().getId() + ": Connecting to hello world server…");

        ZMQ.Socket requester = context.socket(ZMQ.REQ);
        requester.setReceiveTimeOut(10000);
        requester.connect("ipc:///test");

        requester.send("test");

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

        process.waitFor();
    }
}

final class JavaProcess {

    private JavaProcess() {}

    public static Process exec(Class javaClass) throws IOException,
            InterruptedException {
        String javaHome = System.getProperty("java.home");
        String javaBin = javaHome +
                File.separator + "bin" +
                File.separator + "java";
        String classpath = System.getProperty("java.class.path");
        String className = javaClass.getCanonicalName();

        ProcessBuilder builder = new ProcessBuilder(
                javaBin, "-cp", classpath, className, "2000");

        builder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        builder.redirectError(ProcessBuilder.Redirect.INHERIT);

        Process process = builder.start();
        return process;
    }

}
