package de.tuberlin.mcc.geddsprocon;

import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.IDSPSourceConnector;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.IDSPSinkConnector;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.SocketPool;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.flinkconnectors.FlinkSink;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.flinkconnectors.FlinkSource;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.sparkconnectors.SparkSink;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.sparkconnectors.SparkSource;

import java.io.Serializable;

public class DSPConnectorFactory<T extends Serializable> {

    public enum DataStreamProcessors {
        FLINK,
        SPARK
    }

    /*
    @param Class type information needed to serialize tuples, pull or push based Connectors
     */
    public IDSPSourceConnector createSourceConnector(DataStreamProcessors dataStreamProcessor, String host, int port) {
        try {
            return createSourceConnector(dataStreamProcessor, host, port, 0);
        } catch (Exception ex) {
            System.err.println(ex.toString());
        }

        return null;
    }

    /*
    @param Class type information needed to serialize tuples, pull or push based Connectors
     */
    public IDSPSourceConnector createSourceConnector(DataStreamProcessors dataStreamProcessor, String host, int port, int setHWM) {
        try {
            switch(dataStreamProcessor) {
                case FLINK:
                    SocketPool.getInstance().getSocket(SocketPool.SocketType.PULL, host, port, setHWM);
                    return new FlinkSource<T>(host, port);
                case SPARK:
                    SocketPool.getInstance().getSocket(SocketPool.SocketType.PULL, host, port, setHWM);
                    return new SparkSource<T>(host, port);
                default:
                    break;
            }
        } catch (Exception ex) {
            System.err.println(ex.toString());
        }

        return null;
    }

    public IDSPSinkConnector createSinkConnector(DataStreamProcessors dataStreamProcessor, String host, int port) {
        try {
            return createSinkConnector(dataStreamProcessor, host, port, 0);
        } catch (Exception ex) {
            System.err.println(ex.toString());
        }

        return null;
    }

    public IDSPSinkConnector createSinkConnector(DataStreamProcessors dataStreamProcessor, String host, int port, int setHWM) {
        try {
            switch(dataStreamProcessor) {
                case FLINK:
                    SocketPool.getInstance().getSocket(SocketPool.SocketType.PUSH, host, port, setHWM);
                    return new FlinkSink<T>(host, port);
                case SPARK:
                    SocketPool.getInstance().getSocket(SocketPool.SocketType.PUSH, host, port, setHWM);
                    return new SparkSink<>(host, port);
                default:
                    break;
            }
        } catch (Exception ex) {
            System.err.println(ex.toString());
        }

        return null;
    }
}
