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
            return createSourceConnector(dataStreamProcessor, host, port, 1, true);
        } catch (Exception ex) {
            System.err.println(ex.toString());
        }

        return null;
    }

    public IDSPSourceConnector createSourceConnector(DataStreamProcessors dataStreamProcessor, String host, int port, boolean transform) {
        try {
            return createSourceConnector(dataStreamProcessor, host, port, 1, transform);
        } catch (Exception ex) {
            System.err.println(ex.toString());
        }

        return null;
    }

    public IDSPSourceConnector createSourceConnector(DataStreamProcessors dataStreamProcessor, String host, int port, int setHWM) {
        try {
            return createSourceConnector(dataStreamProcessor, host, port, setHWM, true);
        } catch (Exception ex) {
            System.err.println(ex.toString());
        }

        return null;
    }

    /*
    @param Class type information needed to serialize tuples, pull or push based Connectors
     */
    public IDSPSourceConnector createSourceConnector(DataStreamProcessors dataStreamProcessor, String host, int port, int setHWM, boolean transform) {
        try {
            switch(dataStreamProcessor) {
                case FLINK:
                    SocketPool.getInstance().getSocket(SocketPool.SocketType.PULL, host, port, setHWM);
                    return new FlinkSource(host, port, transform);
                case SPARK:
                    SocketPool.getInstance().getSocket(SocketPool.SocketType.PULL, host, port, setHWM);
                    return new SparkSource(host, port, transform);
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
            return createSinkConnector(dataStreamProcessor, host, port, 0, true);
        } catch (Exception ex) {
            System.err.println(ex.toString());
        }

        return null;
    }

    public IDSPSinkConnector createSinkConnector(DataStreamProcessors dataStreamProcessor, String host, int port, int setHWM) {
        try {
            return createSinkConnector(dataStreamProcessor, host, port, setHWM, true);
        } catch (Exception ex) {
            System.err.println(ex.toString());
        }

        return null;
    }

    public IDSPSinkConnector createSinkConnector(DataStreamProcessors dataStreamProcessor, String host, int port, boolean transform) {
        try {
            return createSinkConnector(dataStreamProcessor, host, port, 0, transform);
        } catch (Exception ex) {
            System.err.println(ex.toString());
        }

        return null;
    }

    public IDSPSinkConnector createSinkConnector(DataStreamProcessors dataStreamProcessor, String host, int port, int setHWM, boolean transform) {
        try {
            switch(dataStreamProcessor) {
                case FLINK:
                    SocketPool.getInstance().getSocket(SocketPool.SocketType.PUSH, host, port, setHWM);
                    return new FlinkSink(host, port, transform);
                case SPARK:
                    SocketPool.getInstance().getSocket(SocketPool.SocketType.PUSH, host, port, setHWM);
                    return new SparkSink<>(host, port, transform);
                default:
                    break;
            }
        } catch (Exception ex) {
            System.err.println(ex.toString());
        }

        return null;
    }
}
