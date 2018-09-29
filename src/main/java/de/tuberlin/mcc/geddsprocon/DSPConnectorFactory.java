package de.tuberlin.mcc.geddsprocon;

import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.IDSPSourceConnector;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.IDSPSinkConnector;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.SocketPool;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.flinkconnectors.FlinkSink;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.flinkconnectors.FlinkSource;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.sparkconnectors.SparkSink;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.sparkconnectors.SparkSource;
import de.tuberlin.mcc.geddsprocon.messagebuffer.MessageBuffer;
import de.tuberlin.mcc.geddsprocon.tuple.Tuple2;

public class DSPConnectorFactory<T extends Object> {

    public enum DataStreamProcessors {
        FLINK,
        SPARK
    }

    /*
    @param Class type information needed to serialize tuples, pull or push based Connectors
     */
    public IDSPSourceConnector createSourceConnector(DSPConnectorConfig config) {
        try {
            SocketPool.getInstance().createSockets(config.getSocketType() == SocketPool.SocketType.DEFAULT ? SocketPool.SocketType.REQ : SocketPool.SocketType.PULL, config);
            MessageBuffer.getInstance().initiateBuffer(20);
            switch(config.getDSP()) {
                case FLINK:
                    return new FlinkSource(config.getHost(), config.getPort(), config.getTransform());
                case SPARK:
                    return new SparkSource(config.getHost(), config.getPort(), config.getTransform());
                default:
                    break;
            }
        } catch (Exception ex) {
            System.err.println(ex.toString());
        }

        return null;
    }

    public IDSPSinkConnector createSinkConnector(DSPConnectorConfig config) {
        try {
            SocketPool.getInstance().createSockets(config.getSocketType() == SocketPool.SocketType.DEFAULT ? SocketPool.SocketType.ROUTER : SocketPool.SocketType.PUSH, config);
            MessageBuffer.getInstance().initiateBuffer(20);
            switch(config.getDSP()) {
                case FLINK:
                    FlinkSink sink = new FlinkSink(config);
                    DSPManager manager = new DSPManager(config.getHost(), config.getPort(), sink);
                    MessageBuffer.getInstance().addListener(manager);
                    Thread managerThread = new Thread(manager);
                    managerThread.start();
                    return sink;
                case SPARK:
                    return new SparkSink<>(config);
                default:
                    break;
            }
        } catch (Exception ex) {
            System.err.println(ex.toString());
        }

        return null;
    }
}
