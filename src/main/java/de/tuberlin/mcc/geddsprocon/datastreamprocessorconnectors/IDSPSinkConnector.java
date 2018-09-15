package de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors;

public interface IDSPSinkConnector {
    void sendData(String host, int port, byte[] message);
}
