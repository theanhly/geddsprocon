package de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors;

public interface IDSPConnector {
    void writeBuffer(byte[] byteMessage);
}
