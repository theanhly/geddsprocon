package de.tuberlin.mcc.geddsprocon;

import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.IDSPSourceConnector;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.IDSPSinkConnector;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.SocketPool;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.flinkconnectors.FlinkSink;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.flinkconnectors.FlinkSource;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.sparkconnectors.SparkSink;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.sparkconnectors.SparkSource;
import de.tuberlin.mcc.geddsprocon.messagebuffer.MessageBuffer;

import java.util.HashMap;

public class DSPConnectorFactory<T extends Object> {
    private HashMap<String, MessageBuffer> bufferMap;

    private static DSPConnectorFactory ourInstance = new DSPConnectorFactory();

    public static DSPConnectorFactory getInstance() {
        return ourInstance;
    }

    private DSPConnectorFactory() {
        this.bufferMap = new HashMap<>();
    }

    public enum DataStreamProcessors {
        FLINK,
        SPARK
    }

    /**
     * Connector type enum for source connectors. discuss MASTER instead of PRIMARY
     */
    public class ConnectorType {
        public static final String PRIMARY     = "PRIMARY";
        public static final String SECONDARY   = "SECONDARY";
    }

    public IDSPSourceConnector createSourceConnector(DSPConnectorConfig config) {
        try {
            //SocketPool.getInstance().createSockets(config.getSocketType() == SocketPool.SocketType.DEFAULT ? SocketPool.SocketType.REQ : SocketPool.SocketType.PULL, config);
            IDSPSourceConnector source = null;
            MessageBuffer messageBuffer = null;
            String messageBufferConnectionString = "";
            if(config.getSocketType() == SocketPool.SocketType.REQ || config.getSocketType() == SocketPool.SocketType.DEFAULT) {
                messageBuffer = new MessageBuffer();
                messageBufferConnectionString = messageBuffer.initiateBuffer(config, false);
            }
            this.bufferMap.put(messageBufferConnectionString, messageBuffer);
            switch(config.getDSP()) {
                case FLINK:
                    source =  new FlinkSource(config, messageBufferConnectionString);
                    break;
                case SPARK:
                    source =  new SparkSource(config, messageBufferConnectionString);
                    break;
                default:
                    break;
            }

            if(config.getSocketType() == SocketPool.SocketType.PULL) {
                SocketPool.getInstance().createSockets(SocketPool.SocketType.PULL, config);
            } else if(config.getSocketType() == SocketPool.SocketType.REQ || config.getSocketType() == SocketPool.SocketType.DEFAULT) {
                SocketPool.getInstance().createSockets(SocketPool.SocketType.REQ, config);
                DSPManager.getInstance().initiateBuffer(messageBufferConnectionString).startDSPRequesters(config);
            }

            return source;
        } catch (Exception ex) {
            System.err.println(ex.toString());
        }

        return null;
    }

    public IDSPSinkConnector createSinkConnector(DSPConnectorConfig config) {
        try {
            // if no sockettype is defined use the default socket type
            SocketPool.getInstance().createSockets(config.getSocketType() == SocketPool.SocketType.DEFAULT ? SocketPool.SocketType.ROUTER : SocketPool.SocketType.PUSH, config);
            // initiate the buffer. make it 20 for now for testing purposes. later get the buffer size depending on the hwm (?)
            MessageBuffer messageBuffer = new MessageBuffer();
            String messageBufferConnectionString = messageBuffer.initiateBuffer(config);
            this.bufferMap.put(messageBufferConnectionString, messageBuffer);
            IDSPSinkConnector sink = null;
            switch(config.getDSP()) {
                case FLINK:
                    sink = new FlinkSink(config, messageBufferConnectionString);
                    break;
                case SPARK:
                    sink = new SparkSink<>(config, messageBufferConnectionString);
                default:
                    break;
            }

            // initiate the manager
            DSPRouter router = new DSPRouter(config.getHost(), config.getPort(), sink.getBufferFunction(), messageBufferConnectionString);
            // add the manager as a listener to the message buffer
            messageBuffer.addListener(router);
            // start the manager thread
            Thread routerThread = new Thread(router);
            routerThread.start();
            return sink;
        } catch (Exception ex) {
            System.err.println(ex.toString());
        }

        return null;
    }

    /**
     * The connector factory stores all the buffers which are available to all DSP requesters and routers. This avoids possible issues in the custom sinks and sources.
     * E.g. flink sinks and sources need serializable fields. The message buffer uses ZMQ which isn't completely serializable.
     * @param messageBufferConnectionString
     * @return The requested message buffer
     */
    public MessageBuffer getBuffer(String messageBufferConnectionString) {
        if(this.bufferMap.containsKey(messageBufferConnectionString))
            return this.bufferMap.get(messageBufferConnectionString);

        return null;
    }
}
