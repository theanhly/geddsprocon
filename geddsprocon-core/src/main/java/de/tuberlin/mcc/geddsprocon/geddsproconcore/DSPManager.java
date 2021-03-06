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
import de.tuberlin.mcc.geddsprocon.geddsproconcore.datastreamprocessorconnectors.IDSPInputOperator;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.datastreamprocessorconnectors.IDSPOutputOperator;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.messagebuffer.IMessageBufferListener;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.messagebuffer.MessageBuffer;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple3;

import java.util.ArrayList;
import java.util.HashMap;

public class DSPManager {

    private ArrayList<Tuple3<String, Integer, String>> addresses;
    private String messageBufferConnectionString;
    private HashMap<String, MessageBuffer> inputBufferMap;
    private HashMap<String, MessageBuffer> outputBufferMap;
    private HashMap<String, Thread> dspRouterMap;
    private HashMap<IDSPInputOperator, Thread> inputOpRequesterThreadMap;
    private HashMap<IDSPInputOperator, MessageBuffer> inputOpBufferMap;
    private static final Object dspManagerLock = new Object();
    private static final Object dspRouterLock = new Object();
    private volatile long lastReceivedMessageID;

    private static DSPManager ourInstance = new DSPManager();

    public static DSPManager getInstance() {
        return ourInstance;
    }


    private DSPManager() {
        this.addresses = new ArrayList<>();
        this.inputOpRequesterThreadMap = new HashMap<>();
        this.inputBufferMap = new HashMap<>();
        this.outputBufferMap = new HashMap<>();
        this.inputOpBufferMap = new HashMap<>();
        this.dspRouterMap = new HashMap<>();
        this.lastReceivedMessageID = -1;
    }

    /**
     * Initiates the buffer with the message buffer connection string which is delegated to the DSP requesters.
     * This connection string is needed to pull data from the buffer. Workaround due to serializable issues of
     * the message buffer class.
     * @param messageBufferConnectionString
     * @return DSP manager
     */
    public DSPManager initiateBuffer(String messageBufferConnectionString) {
        this.messageBufferConnectionString = messageBufferConnectionString;
        return this;
    }

    /**
     * Start all DSP requester threads which connect to the output operators and request data.
     * @param config DSP connector config which has all the output operator addresses
     */
    private void startDSPRequesters(DSPConnectorConfig config, MessageBuffer messageBuffer, IDSPInputOperator inputOp) {
        this.addresses = config.getRequestAddresses();

        for(Tuple3<String, Integer, String> tuple : this.addresses) {

            Thread requesterThread = new Thread(new DSPRequester(tuple.f_0, tuple.f_1, tuple.f_2, messageBuffer));
            requesterThread.start();
            this.inputOpRequesterThreadMap.put(inputOp, requesterThread);
        }
    }

    public void initiateInputOperator(DSPConnectorConfig config, IDSPInputOperator inputOp) {
        MessageBuffer messageBuffer = null;
        String messageBufferConnectionString = "";
        for(Tuple3<String, Integer, String> tuple : config.getRequestAddresses())
            messageBufferConnectionString += tuple.f_0 + ":" + tuple.f_1 + ";";

        if(config.getSocketType() == SocketPool.SocketType.REQ || config.getSocketType() == SocketPool.SocketType.DEFAULT) {
            if(Strings.isNullOrEmpty(config.getBufferConnectionString())) {
                if(config.getInputOperatorFaultTolerance()) {
                    if(!this.inputBufferMap.containsKey(messageBufferConnectionString)) {
                        messageBuffer = new MessageBuffer();
                        messageBuffer.initiateBuffer(config, false);
                        this.inputBufferMap.put(messageBufferConnectionString, messageBuffer);
                        System.out.println("using fault tolerance: " );
                    } else {
                        messageBuffer = this.inputBufferMap.get(messageBufferConnectionString);
                        System.out.println("Init buffer mit available message buffer: " + messageBuffer.getMessages());
                    }
                } else {
                    messageBuffer = new MessageBuffer();
                    messageBufferConnectionString = messageBuffer.initiateBuffer(config, false);
                    System.out.println("Init input op: " + Thread.currentThread().getId() + " buffer: " + messageBuffer.hashCode());
                    this.inputOpBufferMap.put(inputOp, messageBuffer);
                }
            } else {
                // deprecated. attempt of using buffer process
                if(this.inputBufferMap.containsKey("ipc:///" + config.getBufferConnectionString()))
                    return;

                messageBuffer = new MessageBuffer();
                messageBufferConnectionString = messageBuffer.initiateBuffer(config, false);
                this.inputBufferMap.put(messageBufferConnectionString, messageBuffer);
            }
        }

        if(config.getSocketType() == SocketPool.SocketType.PULL) {
            SocketPool.getInstance().createSockets(SocketPool.SocketType.PULL, config);
        } else if(config.getSocketType() == SocketPool.SocketType.REQ || config.getSocketType() == SocketPool.SocketType.DEFAULT) {
            SocketPool.getInstance().createSockets(SocketPool.SocketType.REQ, config);
            startDSPRequesters(config, messageBuffer, inputOp);
        }
    }

    public void initiateOutputOperator(DSPConnectorConfig config,  IDSPOutputOperator outputOp) {
        // if no sockettype is defined use the default socket type
        SocketPool.getInstance().createSockets(config.getSocketType() == SocketPool.SocketType.DEFAULT ? SocketPool.SocketType.ROUTER : SocketPool.SocketType.PUSH, config);
        String routerAddress = config.getHost() + ":" + config.getPort();
        String messageBufferString = Strings.isNullOrEmpty(config.getBufferConnectionString()) ? routerAddress : config.getBufferConnectionString();
        MessageBuffer messageBuffer = null;

        if(!this.outputBufferMap.containsKey(messageBufferString)) {
            messageBuffer = new MessageBuffer();
            messageBufferString = messageBuffer.initiateBuffer(config);
            this.outputBufferMap.put(messageBufferString, messageBuffer);
        } else {
            messageBuffer = this.outputBufferMap.get(messageBufferString);
            System.out.println("Buffer content: " + messageBuffer.getMessages());
        }

        // initiate the router if the router only if a router with the same host:port hasn't been started yet
        synchronized (this.getDspRouterLock()) {
            if(!this.dspRouterMap.containsKey(routerAddress)) {
                DSPRouter router = new DSPRouter(config.getHost(), config.getPort(), outputOp.getBufferFunction(), messageBufferString);
                // add the manager as a listener to the message buffer
                messageBuffer.addListener(router);
                if(outputOp instanceof IMessageBufferListener)
                    messageBuffer.addListener((IMessageBufferListener)outputOp);

                // start the router thread
                Thread routerThread = new Thread(router);
                routerThread.start();
                this.dspRouterMap.put(routerAddress, routerThread);
            }
        }
    }

    public void stopRequester(IDSPInputOperator inputOperator) {

        synchronized (this.getDspManagerLock()) {
            if(this.inputOpRequesterThreadMap.containsKey(inputOperator)) {
                Thread requesterThread = this.inputOpRequesterThreadMap.get(inputOperator);
                requesterThread.interrupt();
                try {
                    requesterThread.join();
                    System.err.println("WARNING: Requester thread joined.");
                } catch(InterruptedException ex) {
                    System.err.println("Requester thread interrupted.");
                }
                this.inputOpRequesterThreadMap.remove(inputOperator);
            }
        }
    }

    public void stopRouter(DSPConnectorConfig config) {
        String routerAddress = config.getHost() + ":" + config.getPort();

        synchronized (this.getDspManagerLock()) {
            if(this.dspRouterMap.containsKey(routerAddress)) {
                Thread routerThread = this.dspRouterMap.get(routerAddress);
                routerThread.interrupt();
                try {
                    routerThread.join();
                    System.err.println("WARNING: Router thread joined.");
                } catch(InterruptedException ex) {
                    System.err.println("Router thread interrupted.");
                }
                this.dspRouterMap.remove(routerAddress);
            }
        }
    }

    /**
     * The DSP manager stores all the buffers which are available to all DSP requesters and routers. This avoids possible issues in the custom sinks and sources.
     * E.g. flink sinks and sources need serializable fields. The message buffer uses ZMQ which isn't completely serializable.
     * @param messageBufferConnectionString the connector string starting with 'ipc:///'
     * @return The requested message buffer
     */
    public MessageBuffer getBuffer(String messageBufferConnectionString) {
        if(this.outputBufferMap.containsKey(messageBufferConnectionString))
            return this.outputBufferMap.get(messageBufferConnectionString);

        return null;
    }

    public MessageBuffer getBuffer(String messageBufferConnectionString, IDSPInputOperator inputOp, DSPConnectorConfig config) {
        if(config.getInputOperatorFaultTolerance()) {
            if(this.inputBufferMap.containsKey(messageBufferConnectionString))
                return this.inputBufferMap.get(messageBufferConnectionString);
        } else {
            if(Strings.isNullOrEmpty(messageBufferConnectionString))  {
                if(this.inputOpBufferMap.containsKey(inputOp))
                    return this.inputOpBufferMap.get(inputOp);
            } else {
                if(this.inputBufferMap.containsKey(messageBufferConnectionString))
                    return this.inputBufferMap.get(messageBufferConnectionString);
            }
        }

        return null;
    }

    public long getLastReceivedMessageID() {
        return this.lastReceivedMessageID;
    }

    public void setLastReceivedMessageID(long id) {
        this.lastReceivedMessageID = id;
    }

    public Object getDspManagerLock() {
        return  this.dspManagerLock;
    }

    public Object getDspRouterLock() { return this.dspRouterLock; }
}
