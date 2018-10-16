package de.tuberlin.mcc.geddsprocon;


import com.google.common.base.Strings;
import de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors.SocketPool;
import de.tuberlin.mcc.geddsprocon.tuple.Tuple2;
import de.tuberlin.mcc.geddsprocon.tuple.Tuple3;

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
        // ZMQ_RCVTIMEO, ZMQ_SNDTIMEO
        private final int DEFAULTTIMEOUT = 30000;

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
                withAddress(this.host, this.port);
        }

        @Deprecated
        public Builder withAddress(String host, int port) {
            this.addresses.add(new Tuple2<>(host,port));
            return this;
        }

        public Builder withBufferConnectorString(String connectorString) {
            this.bufferConnectionString = connectorString;
            return this;
        }

        public Builder withRequestAddress(String host, int port, String connectorType) {
            this.requestAddresses.add(new Tuple3<>(host,port, connectorType));
            return this;
        }

        public Builder withDSP(DSPConnectorFactory.DataStreamProcessors dsp) {
            this.dsp = dsp;
            return this;
        }

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

        public Builder withHost(String host) {
            this.host = host;
            return this;
        }

        public Builder withHWM(int hwm) {
            this.hwm = hwm;
            return this;
        }

        public Builder withPort(int port) {
            this.port = port;
            return this;
        }

        @Deprecated
        public Builder withConnectorType(String connectorType) {
            this.connectorType = connectorType;
            return this;
        }

        public Builder withSocketType(SocketPool.SocketType socketType) {
            this.socketType = socketType;
            return this;
        }

        public Builder withoutTransformation() {
            this.transform = false;
            return this;
        }

        public Builder withTimeout(int timeout) {
            this.timeout = timeout;
            return this;
        }

        public DSPConnectorConfig build() {
            DSPConnectorConfig config = new DSPConnectorConfig(this.host, this.port);
            config.addresses = this.addresses;
            config.requestAddresses = this.requestAddresses;
            // first address is the main address. timeout is only set if there are additional addresses. if there are additional addresses but the timeout is smaller than 0 then the default value should be used
            config.timeout = this.addresses.size() > 1 ? this.timeout > -1 ? this.timeout : this.DEFAULTTIMEOUT : -1;
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
