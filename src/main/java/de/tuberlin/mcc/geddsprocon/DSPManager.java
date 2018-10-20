package de.tuberlin.mcc.geddsprocon;

import de.tuberlin.mcc.geddsprocon.messagebuffer.IMessageBufferFunction;
import de.tuberlin.mcc.geddsprocon.tuple.Tuple3;

import java.util.ArrayList;

public class DSPManager {

    private ArrayList<Tuple3<String, Integer, String>> addresses;
    private ArrayList<Thread> requesterThreads;
    private String messageBufferConnectionString;

    private static DSPManager ourInstance = new DSPManager();

    public static DSPManager getInstance() {
        return ourInstance;
    }

    private DSPManager() {
        this.addresses = new ArrayList<>();
        this.requesterThreads = new ArrayList<>();
    }

    /**
     * Initiates the buffer with the message buffer connection string which is delegated to the DSP requesters.
     * This connection string is needed to pull data from the buffer. Workaround due to serializable issues of
     * the message buffer class.
     * @param messageBufferConnectionString
     * @return DSP manager
     */
    public DSPManager initiateBuffer(String messageBufferConnectionString) {
        this.messageBufferConnectionString = messageBufferConnectionString;
        return this;
    }

    /**
     * Start all DSP requester threads which connect to the output operators and request data.
     * @param config DSP connector config which has all the output operator addresses
     */
    public void startDSPRequesters(DSPConnectorConfig config) {
        this.addresses = config.getRequestAddresses();

        for(Tuple3<String, Integer, String> tuple : this.addresses) {

            Thread requesterThread = new Thread(new DSPRequester(tuple.f0, tuple.f1, tuple.f2, this.messageBufferConnectionString));
            requesterThread.start();
            this.requesterThreads.add(requesterThread);
        }
    }
}
