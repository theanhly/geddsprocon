package de.tuberlin.mcc.geddsprocon.geddsproconcore;


import com.google.common.base.Strings;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.datastreamprocessorconnectors.SocketPool;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple2;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple3;

import java.io.Serializable;
import java.util.ArrayList;

public class DSPConnectorConfig implements Serializable {
    private ArrayList<Tuple2<String, Integer>> addresses;
    private ArrayList<Tuple3<String, Integer, String>> requestAddresses;
    private DSPConnectorFactory.DataStreamProcessors dsp;
    private int hwm;
    private String host;
    private int port;
    private int timeout;
    private boolean transform;
    private String connectorType;
    private String bufferConnectionString;
    private SocketPool.SocketType socketType;

    private DSPConnectorConfig() {
        this("", -1);
    }

    private DSPConnectorConfig(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public ArrayList<Tuple2<String, Integer>> getAddresses() { return addresses; }

    public ArrayList<Tuple3<String, Integer, String>> getRequestAddresses() { return requestAddresses; }

    public DSPConnectorFactory.DataStreamProcessors getDSP() {
        return this.dsp;
    }

    public String getHost() {
        return this.host;
    }

    public int getHwm() {
        return this.hwm;
    }

    public int getPort() {
        return this.port;
    }

    public String getConncetorType() { return this.connectorType; }

    public String getBufferConnectionString() { return this.bufferConnectionString; }

    public SocketPool.SocketType getSocketType() {
        return this.socketType;
    }

    public int getTimeout() { return timeout; }

    public boolean getTransform() {
        return this.transform;
    }

    public static class Builder implements Serializable {
        // timeout is half a minute. if the data cannot be sent or received to or from the socket it will return with an error.
        private final int DEFAULTTIMEOUT = 5000;

        private ArrayList<Tuple2<String, Integer>> addresses;
        private ArrayList<Tuple3<String, Integer, String>> requestAddresses;
        private DSPConnectorFactory.DataStreamProcessors dsp = null;
        private String host;
        private int hwm = 1000;
        private int port;
        private int timeout = -1;
        private boolean transform = true;
        private String connectorType = DSPConnectorFactory.ConnectorType.PRIMARY;
        private String bufferConnectionString = "";
        private SocketPool.SocketType socketType = SocketPool.SocketType.DEFAULT;

        public Builder() {
            this("", -1);
        }

        public Builder(String host, int port) {
            this.host = host;
            this.port = port;
            this.addresses = new ArrayList<>();
            this.requestAddresses = new ArrayList<>();
            if(!Strings.isNullOrEmpty(this.host) && this.port > 0)
                this.addresses.add(new Tuple2<>(host,port));
        }

        /**
         * Providing a buffer connector string allows potential reconnects to an existing buffer in case the application fails. This provides some sort of
         * persistance.
         * @param connectorString Connector string
         * @return Connector config builder
         */
        public Builder withBufferConnectorString(String connectorString) {
            this.bufferConnectionString = connectorString;
            return this;
        }

        /**
         * Adds the request address for an input operator. The connector type determines if the connection to the output operator should be primary or secondary.
         * A primary connection receives the messages immediately from the output operator while a secondary connection only receives messages when the buffer is full.
         *
         * @param host
         * @param port
         * @param connectorType
         * @return Connector config builder
         */
        public Builder withRequestAddress(String host, int port, String connectorType) {
            this.requestAddresses.add(new Tuple3<>(host,port, connectorType));
            return this;
        }

        /**
         * Determines which data stream process is used to create the connectors.
         * @param dsp
         * @return Connector config builder
         */
        public Builder withDSP(DSPConnectorFactory.DataStreamProcessors dsp) {
            this.dsp = dsp;
            return this;
        }

        /**
         * Determines which data stream process is used to create the connectors. For consistency reasons method {@link #withDSP(DSPConnectorFactory.DataStreamProcessors)}
         * should be the preferred method.
         * @param dspString
         * @return Connector config builder
         */
        public Builder withDSP(String dspString) {
            switch(dspString.toLowerCase()) {
                case "flink":
                    this.dsp = DSPConnectorFactory.DataStreamProcessors.FLINK;
                    break;
                case "spark":
                    this.dsp = DSPConnectorFactory.DataStreamProcessors.SPARK;
                    break;
                default:
                    throw new IllegalArgumentException("DSP not found");
            }
            return this;
        }

        /**
         * HWM is derived from ZeroMQ's 'high water mark' which determines the messages which can be in queue.
         * We use it a similar fashion and HWM determines the maximum amount of tuples which should be stored in the buffer
         * @param hwm
         * @return Connector config builder
         */
        public Builder withHWM(int hwm) {
            this.hwm = hwm;
            return this;
        }

        /**
         * Mainly for test purposes. Default socket types should always be ROUTER and DEALER sockets
         * @param socketType
         * @return Connector config builder
         */
        public Builder withSocketType(SocketPool.SocketType socketType) {
            this.socketType = socketType;
            return this;
        }

        /**
         * If a transformation from and to the intermediate tuple is not needed, i.e. sending and receiving from the same DSP framework (e.g. from flink to flink), then transformation can be turned of.
         * @return Connector config builder
         */
        public Builder withoutTransformation() {
            this.transform = false;
            return this;
        }

        /**
         * The timeout determines how long an operator is waiting for a receive or sending. Depending of the operator type (output or input) it could mean different things:
         * Output operator: If the timeout is reached and the output operator wasn't able to send the message, the output operator drops the message. The sending has failed and the message will be sent to the next
         * available input operator.
         * Input operator: If the timout is reached and the input operator wasn't able to receive from an output operator it could mean that the output operator went down in the meantime. To ensure that the output operator
         * knows about the availability of the input operator we set a timeout to frequently sent requests.
         * @param timeout Timeout in ms
         * @return Connector config builder
         */
        public Builder withTimeout(int timeout) {
            this.timeout = timeout;
            return this;
        }

        public DSPConnectorConfig build() {
            DSPConnectorConfig config = new DSPConnectorConfig(this.host, this.port);
            config.addresses = this.addresses;
            config.requestAddresses = this.requestAddresses;
            // first address is the main address. timeout is only set if there are additional addresses. if there are additional addresses but the timeout is smaller than 0 then the default value should be used
            config.timeout = this.timeout > -1 ? this.timeout : this.DEFAULTTIMEOUT;
            if(this.dsp == null)
                throw new IllegalArgumentException("Need to define a DSP.");
            config.dsp = this.dsp;
            config.transform = this.transform;
            config.hwm = this.hwm;
            config.connectorType = this.connectorType;
            config.socketType = this.socketType;
            config.bufferConnectionString = this.bufferConnectionString;
            return config;
        }
    }
}
