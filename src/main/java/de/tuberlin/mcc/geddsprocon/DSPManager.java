package de.tuberlin.mcc.geddsprocon;

import de.tuberlin.mcc.geddsprocon.messagebuffer.IMessageBufferFunction;
import de.tuberlin.mcc.geddsprocon.tuple.Tuple3;

import java.util.ArrayList;

public class DSPManager {

    private ArrayList<Tuple3<String, Integer, String>> addresses;
    private ArrayList<Thread> requesterThreads;

    private static DSPManager ourInstance = new DSPManager();

    public static DSPManager getInstance() {
        return ourInstance;
    }

    private DSPManager() {
        this.addresses = new ArrayList<>();
        this.requesterThreads = new ArrayList<>();
    }

    public void startDSPRequesters(DSPConnectorConfig config) {
        this.addresses = config.getRequestAddresses();

        for(Tuple3<String, Integer, String> tuple : this.addresses) {

            Thread requesterThread = new Thread(new DSPRequester(tuple.f0, tuple.f1, tuple.f2));
            requesterThread.start();
            this.requesterThreads.add(requesterThread);
        }
    }
}
