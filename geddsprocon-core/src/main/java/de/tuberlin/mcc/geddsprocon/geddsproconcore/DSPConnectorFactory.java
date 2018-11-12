package de.tuberlin.mcc.geddsprocon.geddsproconcore;

import de.tuberlin.mcc.geddsprocon.geddsproconcore.datastreamprocessorconnectors.IDSPInputOperator;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.datastreamprocessorconnectors.IDSPOutputOperator;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.datastreamprocessorconnectors.flinkconnectors.FlinkOutputOperator;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.datastreamprocessorconnectors.flinkconnectors.FlinkInputOperator;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.datastreamprocessorconnectors.sparkconnectors.SparkOutputOperator;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.datastreamprocessorconnectors.sparkconnectors.SparkInputOperator;


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

    public IDSPInputOperator createInputOperator(DSPConnectorConfig config) {
        try {
            switch(config.getDSP()) {
                case FLINK:
                    return  new FlinkInputOperator(config);
                case SPARK:
                    return new SparkInputOperator(config);
                default:
                    break;
            }
        } catch (Exception ex) {
            System.err.println(ex.toString());
        }

        return null;
    }

    public IDSPOutputOperator createOutputOperator(DSPConnectorConfig config) {
        try {
            switch(config.getDSP()) {
                case FLINK:
                    return new FlinkOutputOperator(config);
                case SPARK:
                    return new SparkOutputOperator<>(config);
                default:
                    break;
            }
        } catch (Exception ex) {
            System.err.println(ex.toString());
        }

        return null;
    }
}
