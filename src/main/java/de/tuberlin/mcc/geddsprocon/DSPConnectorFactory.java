package de.tuberlin.mcc.geddsprocon;

import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.IDSPSourceConnector;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.IDSPSinkConnector;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.SocketPool;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.flinkconnectors.FlinkSink;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.flinkconnectors.FlinkSource;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.sparkconnectors.SparkSink;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.sparkconnectors.SparkSource;
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
            SocketPool.getInstance().createSockets(SocketPool.SocketType.PULL, config);
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
            SocketPool.getInstance().createSockets(SocketPool.SocketType.PUSH, config);

            switch(config.getDSP()) {
                case FLINK:
                    return new FlinkSink(config);
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
