package de.tuberlin.mcc.geddsprocon;

import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.IDSPSourceConnector;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.IDSPSinkConnector;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.flinkconnectors.FlinkSink;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.flinkconnectors.FlinkSource;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.sparkconnectors.SparkSink;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.sparkconnectors.SparkSource;


public class DSPConnectorFactory<T extends Object> {

    private static DSPConnectorFactory ourInstance = new DSPConnectorFactory();

    public static DSPConnectorFactory getInstance() {
        return ourInstance;
    }

    private DSPConnectorFactory() {    }

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
            switch(config.getDSP()) {
                case FLINK:
                    return  new FlinkSource(config);
                case SPARK:
                    return new SparkSource(config);
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
