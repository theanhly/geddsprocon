package de.tuberlin.mcc.geddsprocon;

import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.IDSPSinkConnector;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.IDSPSourceConnector;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.SocketPool;
import de.tuberlin.mcc.geddsprocon.messagebuffer.IMessageBufferFunction;
import de.tuberlin.mcc.geddsprocon.messagebuffer.MessageBuffer;
import de.tuberlin.mcc.geddsprocon.tuple.Tuple3;

import java.util.ArrayList;
import java.util.HashMap;

public class DSPManager {

    private ArrayList<Tuple3<String, Integer, String>> addresses;
    private ArrayList<Thread> requesterThreads;
    private String messageBufferConnectionString;
    private HashMap<String, MessageBuffer> bufferMap;

    private static DSPManager ourInstance = new DSPManager();

    public static DSPManager getInstance() {
        return ourInstance;
    }

    private DSPManager() {
        this.addresses = new ArrayList<>();
        this.requesterThreads = new ArrayList<>();
        this.bufferMap = new HashMap<>();
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
    public void startDSPRequesters(DSPConnectorConfig config) {
        this.addresses = config.getRequestAddresses();

        for(Tuple3<String, Integer, String> tuple : this.addresses) {

            Thread requesterThread = new Thread(new DSPRequester(tuple.f0, tuple.f1, tuple.f2, this.messageBufferConnectionString));
            requesterThread.start();
            this.requesterThreads.add(requesterThread);
        }
    }

    public void initiateInputOperator(DSPConnectorConfig config) {
        MessageBuffer messageBuffer = null;
        String messageBufferConnectionString = "";
        if(config.getSocketType() == SocketPool.SocketType.REQ || config.getSocketType() == SocketPool.SocketType.DEFAULT) {
            messageBuffer = new MessageBuffer();
            messageBufferConnectionString = messageBuffer.initiateBuffer(config, false);
        }
        this.bufferMap.put(messageBufferConnectionString, messageBuffer);

        if(config.getSocketType() == SocketPool.SocketType.PULL) {
            SocketPool.getInstance().createSockets(SocketPool.SocketType.PULL, config);
        } else if(config.getSocketType() == SocketPool.SocketType.REQ || config.getSocketType() == SocketPool.SocketType.DEFAULT) {
            SocketPool.getInstance().createSockets(SocketPool.SocketType.REQ, config);
            DSPManager.getInstance().initiateBuffer(messageBufferConnectionString).startDSPRequesters(config);
        }
    }

    public void initiateOutputOperator(DSPConnectorConfig config,  IDSPSinkConnector sink) {
        // if no sockettype is defined use the default socket type
        SocketPool.getInstance().createSockets(config.getSocketType() == SocketPool.SocketType.DEFAULT ? SocketPool.SocketType.ROUTER : SocketPool.SocketType.PUSH, config);
        // initiate the buffer. make it 20 for now for testing purposes. later get the buffer size depending on the hwm (?)
        MessageBuffer messageBuffer = new MessageBuffer();
        String messageBufferConnectionString = messageBuffer.initiateBuffer(config);
        this.bufferMap.put(messageBufferConnectionString, messageBuffer);

        // initiate the manager
        DSPRouter router = new DSPRouter(config.getHost(), config.getPort(), sink.getBufferFunction(), messageBufferConnectionString);
        // add the manager as a listener to the message buffer
        messageBuffer.addListener(router);
        // start the manager thread
        Thread routerThread = new Thread(router);
        routerThread.start();
    }

    /**
     * The DSP manager stores all the buffers which are available to all DSP requesters and routers. This avoids possible issues in the custom sinks and sources.
     * E.g. flink sinks and sources need serializable fields. The message buffer uses ZMQ which isn't completely serializable.
     * @param messageBufferConnectionString the connector string starting with 'ipc:///'
     * @return The requested message buffer
     */
    public MessageBuffer getBuffer(String messageBufferConnectionString) {
        if(this.bufferMap.containsKey(messageBufferConnectionString))
            return this.bufferMap.get(messageBufferConnectionString);

        return null;
    }
}
