package de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors;

public interface IDSPSourceConnector {
    byte[] receiveData(String host, int port);
}
