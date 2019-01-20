/*
 * Copyright 2019 The-Anh Ly
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.tuberlin.mcc.geddsprocon.geddsproconcore;

import com.google.common.base.Strings;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.messagebuffer.MessageBuffer;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;
import org.zeromq.ZMsg;


public class DSPRequester implements Runnable {
    private String host;
    private int port;
    private String connectorType;
    private long messageNumber;
    private String messageBufferConnectionString;
    private MessageBuffer messageBuffer;
    private boolean isRunning;

    public DSPRequester(String host, int port, String connectorType, MessageBuffer messageBuffer) {
        this.host = host;
        this.port = port;
        this.connectorType = connectorType;
        this.messageBufferConnectionString = messageBufferConnectionString;
        this.messageNumber = -1;
        this.messageBuffer = messageBuffer;
        this.isRunning = true;
    }

    /**
     * Runs the DSP requester thread.
     */
    @Override
    public void run() {
        System.out.println("Starting requester with connection to: " + this.host + ":" +  this.port + " @Thread-ID: " + Thread.currentThread().getId());

        try {
            ZMQ.Socket socket;
            while(this.isRunning || !Thread.interrupted()) {

                // might need to lock until receive. can cause ZMQException where it receives while the socket is used by the other requester.
                // alternative way to send a multipart message
                socket = SocketPool.getInstance().getOrCreateSocket(this.host, this.port);
                ZMsg messages = new ZMsg();
                if(socket != null) {
                    while(messages == null || messages.peek() == null || Strings.isNullOrEmpty(messages.peek().toString())) {
                        socket.send(this.connectorType, ZMQ.SNDMORE);
                        socket.send(Long.toString(DSPManager.getInstance().getLastReceivedMessageID()), ZMQ.DONTWAIT);

                        messages = ZMsg.recvMsg(socket);

                        if(messages != null && !Strings.isNullOrEmpty(messages.peek().toString())) {
                            DSPManager.getInstance().setLastReceivedMessageID(Long.parseLong(messages.pop().toString()));
                        }
                    }
                    SocketPool.getInstance().checkInSocket(this.host, this.port);

                    if(messages != null && !Strings.isNullOrEmpty(messages.peek().toString())) {
                        for(ZFrame frame : messages) {
                            // block writing to buffer as long the buffer is full
                            while(this.messageBuffer.isFull()) {}

                            this.messageBuffer.writeBuffer(frame.getData());
                        }
                    }
                }
            }
            System.err.println("Stopping requester,,, after");
        } catch(ZMQException ex) {
            System.err.println("ZMQException in thread " + Thread.currentThread().getId());
            System.err.println(ex.toString());
            System.err.println(ex.getStackTrace());
        }
    }

    public void stop() {
        System.err.println("requester requester,,, ");
        this.isRunning = false;
    }
}
