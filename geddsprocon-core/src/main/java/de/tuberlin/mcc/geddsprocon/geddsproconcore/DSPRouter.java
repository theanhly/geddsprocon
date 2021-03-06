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
import de.tuberlin.mcc.geddsprocon.geddsproconcore.messagebuffer.IMessageBufferFunction;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.messagebuffer.IMessageBufferListener;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.messagebuffer.MessageBuffer;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.util.HashSet;
import java.util.LinkedList;

public class DSPRouter implements Runnable, IMessageBufferListener {

    private String host;
    private int port;
    private String routerAdress;
    private volatile IMessageBufferFunction bufferFunction;
    private volatile LinkedList<ZFrame> endpointQueue;
    private volatile HashSet<ZFrame> endpointSet;
    private final ZMQ.Socket socket;
    private ZFrame temporaryPrimary;
    private boolean resendPrevBuffer;
    private int lastReceivedMessageNumber;
    private String messageBufferConnectionString;
    private boolean isRunning;

    public DSPRouter(String host, int port, IMessageBufferFunction bufferFunction, String messageBufferConnectionString) {
        this.host = host;
        this.port = port;
        this.messageBufferConnectionString = messageBufferConnectionString;
        this.routerAdress = Strings.isNullOrEmpty(messageBufferConnectionString) ? host + ":" + port : messageBufferConnectionString;
        this.bufferFunction = bufferFunction;
        this.endpointQueue = new LinkedList<>();
        this.endpointSet = new HashSet<>();
        this.socket = SocketPool.getInstance().getOrCreateSocket(this.host, this.port);
        this.resendPrevBuffer = false;
        this.lastReceivedMessageNumber = -1;
        this.isRunning = true;
    }

    @Override
    public void run() {
        System.out.println("Starting router @" + routerAdress);

        while(this.isRunning || !Thread.interrupted()) {
            ZMsg msg = ZMsg.recvMsg(this.socket);

            if(msg != null && !msg.isEmpty()) {
                //  First frame is address
                ZFrame address = msg.pop();

                //  Second frame is empty in a REQ socket. Second frame of DEALER socket is not empty
                String empty = new String(msg.pop().getData());

                assert (empty.length() == 0);

                String ready = new String(msg.pop().getData());
                assert(ready.length() > 0);

                if(this.temporaryPrimary != null && this.temporaryPrimary.hasData() && this.temporaryPrimary.getData().equals(address.getData()))
                    this.lastReceivedMessageNumber = Integer.parseInt(msg.pop().toString());

                if(ready.equals(DSPConnectorFactory.ConnectorType.PRIMARY)) {

                    this.temporaryPrimary = address.duplicate();

                    if(!reply(address)) {
                        this.temporaryPrimary = null;
                        System.out.println("Message could not be sent.");
                    }
                } else if(ready.equals(DSPConnectorFactory.ConnectorType.SECONDARY)) {

                    if(this.temporaryPrimary != null && this.temporaryPrimary.hasData() && this.temporaryPrimary.getData().equals(address.getData()))
                        reply(address);
                    else if(DSPManager.getInstance().getBuffer(this.host + ":" + this.port).isFull()/*DSPManager.getInstance().getBuffer(this.messageBufferConnectionString).isFull()*/) {
                        this.temporaryPrimary = address.duplicate();
                        reply(address);
                    }
                    else if(this.endpointSet.add(address))
                        this.endpointQueue.add(address);
                }
            }
        }
        System.err.println("requester router,,, after");
    }

    /**
     * go through the queue and send to the addresses in the queue
     * @return return true if sending was successful
     */
    private synchronized boolean reply() {
        if(DSPManager.getInstance().getBuffer(this.routerAdress).isFull() &&  this.endpointQueue != null && this.endpointQueue.size() > 0) {
            ZFrame addressFrame = null;
            do {
                addressFrame = this.endpointQueue.pop();
                this.temporaryPrimary = addressFrame.duplicate();
                this.endpointSet.remove(addressFrame);
            } while(addressFrame != null && !reply(addressFrame));

            System.out.println("Reply sent to " + addressFrame.toString());
            return true;
        }

        return false;
    }

    /**
     * reply to the address by flushing the message buffer
     * @param address
     * @return
     */
    private boolean reply(ZFrame address) {
        ZMsg message = new ZMsg();
        message.add(address);

        // DEALER socket doesn't need an empty second frame
        message.add("");

        boolean resendPrevBuffer = false;

        long currentMessages = DSPManager.getInstance().getBuffer(this.routerAdress).getSentMessages();

        if(currentMessages > 1 && (this.lastReceivedMessageNumber + 1 != currentMessages || (this.lastReceivedMessageNumber == MessageBuffer.RESETMESSAGENUMBER && currentMessages != 1))) {
            resendPrevBuffer = true;
        }

        if(!resendPrevBuffer && DSPManager.getInstance().getBuffer(this.routerAdress).isEmpty()) {
            message.add("");
            return message.send(this.socket);
        }

        Object bufferLock = DSPManager.getInstance().getBuffer(this.routerAdress).getBufferLock();
        synchronized (bufferLock) {
            if(resendPrevBuffer)
                System.out.println(Thread.currentThread().getId() + " ( Thread ID): Resending buffer: " + this.lastReceivedMessageNumber);
            ZMsg buffermsg = DSPManager.getInstance().getBuffer(this.routerAdress).flushBuffer(this.bufferFunction, false, resendPrevBuffer);
            message.append(buffermsg);
            if(message.send(this.socket)) {
                if(!resendPrevBuffer)
                    DSPManager.getInstance().getBuffer(this.routerAdress).clearBuffer();

                return true;
            } else {
                System.err.println("Sending not successful.");
                return false;
            }
        }
    }

    /**
     * is used when the buffer is full. go through the queue and find an address to reply to
     */
    @Override
    public void bufferIsFullEvent() {
        if(DSPManager.getInstance().getBuffer(this.routerAdress).isFull())
            reply();
    }

    public void stop() {
        System.err.println("Stopping router,,,");
        this.isRunning = false;
    }
}