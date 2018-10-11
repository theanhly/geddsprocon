package de.tuberlin.mcc.geddsprocon;

import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.IDSPSourceConnector;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.IDSPSinkConnector;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.SocketPool;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.flinkconnectors.FlinkSink;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.flinkconnectors.FlinkSource;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.flinkconnectors.TupleTransformer;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.sparkconnectors.SparkSink;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.sparkconnectors.SparkSource;
import de.tuberlin.mcc.geddsprocon.messagebuffer.MessageBuffer;
import org.apache.commons.lang.SerializationUtils;

import java.io.Serializable;

public class DSPConnectorFactory<T extends Object> {

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
            MessageBuffer.getInstance().initiateBuffer(20);
            IDSPSourceConnector source = null;
            switch(config.getDSP()) {
                case FLINK:
                    source =  new FlinkSource(config);
                    break;
                case SPARK:
                    source =  new SparkSource(config);
                    break;
                default:
                    break;
            }

            if(config.getSocketType() == SocketPool.SocketType.PULL) {
                SocketPool.getInstance().createSockets(SocketPool.SocketType.PULL, config);
            } else if(config.getSocketType() == SocketPool.SocketType.REQ || config.getSocketType() == SocketPool.SocketType.DEFAULT) {
                SocketPool.getInstance().createSockets(SocketPool.SocketType.REQ, config);
                DSPManager.getInstance().startDSPRequesters(config);
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
            MessageBuffer.getInstance().initiateBuffer(20);
            IDSPSinkConnector sink = null;
            switch(config.getDSP()) {
                case FLINK:
                    sink = new FlinkSink(config);
                    break;
                case SPARK:
                    sink = new SparkSink<>(config);
                default:
                    break;
            }

            // initiate the manager
            DSPRouter router = new DSPRouter(config.getHost(), config.getPort(), sink.getBufferFunction());
            // add the manager as a listener to the message buffer
            MessageBuffer.getInstance().addListener(router);
            // start the manager thread
            Thread routerThread = new Thread(router);
            routerThread.start();
            return sink;
        } catch (Exception ex) {
            System.err.println(ex.toString());
        }

        return null;
    }
}
