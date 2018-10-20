package de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors;

public interface IDSPSourceConnector {

    /**
     * Method determines the way the input operator (source) is receiving the data from an output operator (sink)
     * @param host Host name of the input operator (source)
     * @param port Port of the input operator (source)
     * @return
     */
    byte[] receiveData(String host, int port);
}
