package de.tuberlin.mcc.geddsprocon.geddsproconcore;

import com.google.common.base.Strings;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.datastreamprocessorconnectors.IDSPInputOperator;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.datastreamprocessorconnectors.IDSPOutputOperator;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.datastreamprocessorconnectors.SocketPool;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.messagebuffer.MessageBuffer;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple3;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class DSPManager {

    private ArrayList<Tuple3<String, Integer, String>> addresses;
    private ArrayList<Thread> requesterThreads;
    private String messageBufferConnectionString;
    private HashMap<String, MessageBuffer> bufferProcessMap;
    private HashMap<String, DSPRouter> dspRouterMap;
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
        this.requesterThreads = new ArrayList<>();
        this.bufferProcessMap = new HashMap<>();
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
    public void startDSPRequesters(DSPConnectorConfig config, MessageBuffer messageBuffer) {
        this.addresses = config.getRequestAddresses();

        for(Tuple3<String, Integer, String> tuple : this.addresses) {

            Thread requesterThread = new Thread(new DSPRequester(tuple.f0, tuple.f1, tuple.f2, messageBuffer));
            requesterThread.start();
            this.requesterThreads.add(requesterThread);
        }
    }

    public void initiateInputOperator(DSPConnectorConfig config, IDSPInputOperator inputOp) {
        MessageBuffer messageBuffer = null;
        String messageBufferConnectionString = "";
        if(config.getSocketType() == SocketPool.SocketType.REQ || config.getSocketType() == SocketPool.SocketType.DEFAULT) {
            if(Strings.isNullOrEmpty(config.getBufferConnectionString())) {
                messageBuffer = new MessageBuffer();
                messageBufferConnectionString = messageBuffer.initiateBuffer(config, false);
                System.out.println("Init input op: " + Thread.currentThread().getId() + " buffer: " + messageBuffer.hashCode());
                this.inputOpBufferMap.put(inputOp, messageBuffer);
            } else {
                if(this.bufferProcessMap.containsKey("ipc:///" + config.getBufferConnectionString()))
                    return;

                messageBuffer = new MessageBuffer();
                messageBufferConnectionString = messageBuffer.initiateBuffer(config, false);
                this.bufferProcessMap.put(messageBufferConnectionString, messageBuffer);
            }
        }

        if(config.getSocketType() == SocketPool.SocketType.PULL) {
            SocketPool.getInstance().createSockets(SocketPool.SocketType.PULL, config);
        } else if(config.getSocketType() == SocketPool.SocketType.REQ || config.getSocketType() == SocketPool.SocketType.DEFAULT) {
            SocketPool.getInstance().createSockets(SocketPool.SocketType.REQ, config);
            startDSPRequesters(config, messageBuffer);
        }
    }

    public void initiateOutputOperator(DSPConnectorConfig config,  IDSPOutputOperator outputOp) {
        // if no sockettype is defined use the default socket type
        SocketPool.getInstance().createSockets(config.getSocketType() == SocketPool.SocketType.DEFAULT ? SocketPool.SocketType.ROUTER : SocketPool.SocketType.PUSH, config);
        // initiate the buffer. make it 20 for now for testing purposes. later get the buffer size depending on the hwm (?)
        String messageBufferString = "";
        String routerAddress = config.getHost() + ":" + config.getPort();
        MessageBuffer messageBuffer = null;

        if(!this.bufferProcessMap.containsKey(messageBufferString)) {
            //String messageBufferConnectionString = messageBuffer.initiateBuffer(config);
            //this.bufferProcessMap.put(messageBufferConnectionString, messageBuffer);
            messageBuffer = new MessageBuffer();
            messageBufferString = messageBuffer.initiateBuffer(config);
            this.bufferProcessMap.put(messageBufferString, messageBuffer);
        } else
            messageBuffer = this.bufferProcessMap.get(messageBufferString);

        // initiate the router if the router only if a router with the same host:port hasn't been started yet
        synchronized (this.getDspManagerLock()) {
            if(!this.dspRouterMap.containsKey(routerAddress)) {
                System.out.println("Starting router");
                DSPRouter router = new DSPRouter(config.getHost(), config.getPort(), outputOp.getBufferFunction(), messageBufferString);
                // add the manager as a listener to the message buffer
                messageBuffer.addListener(router);
                // start the manager thread
                Thread routerThread = new Thread(router);
                routerThread.start();
                this.dspRouterMap.put(routerAddress, router);
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
        if(this.bufferProcessMap.containsKey(messageBufferConnectionString))
            return this.bufferProcessMap.get(messageBufferConnectionString);

        return null;
    }

    public MessageBuffer getBuffer(String messageBufferConnectionString, IDSPInputOperator inputOp) {
        if(Strings.isNullOrEmpty(messageBufferConnectionString))  {
            if(this.inputOpBufferMap.containsKey(inputOp))
                return this.inputOpBufferMap.get(inputOp);
        } else {
            if(this.bufferProcessMap.containsKey(messageBufferConnectionString))
                return this.bufferProcessMap.get(messageBufferConnectionString);
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
