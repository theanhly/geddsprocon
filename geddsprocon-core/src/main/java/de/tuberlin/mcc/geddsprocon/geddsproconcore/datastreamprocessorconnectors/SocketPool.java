package de.tuberlin.mcc.geddsprocon.geddsproconcore.datastreamprocessorconnectors;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;

import de.tuberlin.mcc.geddsprocon.geddsproconcore.DSPConnectorConfig;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple2;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple3;
import org.apache.commons.lang.NullArgumentException;
import org.apache.commons.lang.SerializationUtils;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

public class SocketPool {
    public enum SocketType { PULL, PUSH, PUB, SUB, REP, REQ, ROUTER, DEALER, DEFAULT };

    private static SocketPool socketFactoryInstance = new SocketPool();

    public static SocketPool getInstance() {
        return socketFactoryInstance;
    }


    private volatile ConcurrentHashMap<String, ZMQ.Socket> sockets;
    private volatile ConcurrentHashMap<String, ZMQ.Socket> checkedOutSockets;
    private static volatile ZMQ.Context context;

    private SocketPool() {
        this.sockets = new ConcurrentHashMap<>();
        this.checkedOutSockets = new ConcurrentHashMap<>();
        this.context = ZMQ.context(1);
    }

    private static final String IPADDRESS_PATTERN =
            "^([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
                    "([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
                    "([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
                    "([01]?\\d\\d?|2[0-4]\\d|25[0-5])$";

    public synchronized void checkInSocket(String host, int port) {
        String key = host + ":" + port;
        ZMQ.Socket clientSocket = this.checkedOutSockets.get(key);
        this.sockets.put(key, clientSocket);
        this.checkedOutSockets.remove(key);
    }

    public synchronized ZMQ.Socket getOrCreateSocket(String host, int port) throws IllegalArgumentException {
        return getOrCreateSocket(null, host, port, null);
    }

    public synchronized ZMQ.Socket getOrCreateSocket(SocketType socketType, String host, int port, DSPConnectorConfig config) throws IllegalArgumentException {

        // TODO: use regex to validate host

        String key = host + ":" + port;
        if(this.sockets.containsKey(key)) {
            ZMQ.Socket clientSocket = this.sockets.get(key);
            this.checkedOutSockets.put(key, clientSocket);
            this.sockets.remove(key);
            return clientSocket;
        } else if (this.checkedOutSockets.containsKey(key)){
            return null;
        } else {
            if(config == null) {
                throw new NullArgumentException("DSP connector config needs to be defined");
            }

            if(socketType == null) {
                throw new NullArgumentException("Scocket type needs to be defined");
            }

            return createSocket(socketType, host, port, config);
        }
    }

    public synchronized void createSockets(SocketType socketType, DSPConnectorConfig config) {
        for(Tuple2<String, Integer> tuple : config.getAddresses()) {
            createSocket(socketType, tuple.f_0, tuple.f_1, config);
        }

        for(Tuple3<String, Integer, String> tuple : config.getRequestAddresses()) {
            createSocket(socketType, tuple.f_0, tuple.f_1, config);
        }
    }

    private synchronized ZMQ.Socket createSocket(SocketType socketType, String host, int port, DSPConnectorConfig config) {
        String key = host + ":" + port;

        ZMQ.Socket socket = null;

        if(this.sockets.containsKey(key))
            return this.sockets.get(key);

        if(this.checkedOutSockets.containsKey(key))
            return this.checkedOutSockets.get(key);

        if (socketType == null)
            throw new IllegalArgumentException(String.format("Socket with host {0} and port {1} not found. Parameter socketType needs to be defined.", host, port));

        switch(socketType) {
            case PULL:
                socket = this.context.socket(ZMQ.PULL);
                socket.setReceiveTimeOut(config.getTimeout());
                socket.setRcvHWM(config.getHwm());
                socket.bind("tcp://"+  key);
                break;
            case SUB:
                socket = this.context.socket(ZMQ.SUB);
                socket.setReceiveTimeOut(config.getTimeout());
                socket.setRcvHWM(config.getHwm());
                socket.connect("tcp://"+  key);
                break;
            case PUSH:
                socket = this.context.socket(ZMQ.PUSH);
                socket.setSendTimeOut(config.getTimeout());
                socket.setSndHWM(config.getHwm());
                socket.setImmediate(true);
                socket.setSendBufferSize(1);
                socket.connect("tcp://"+  key);
                break;
            case PUB:
                socket = this.context.socket(ZMQ.PUB);
                socket.setSendTimeOut(config.getTimeout());
                socket.setSndHWM(config.getHwm());
                socket.setImmediate(true);
                socket.bind("tcp://"+  key);
                break;
            case REP:
                socket = this.context.socket(ZMQ.REP);
                socket.setSendTimeOut(config.getTimeout());
                socket.setImmediate(true);
                socket.bind("tcp://"+  key);
                break;
            case REQ:
                socket = this.context.socket(ZMQ.REQ);
                socket.setReceiveTimeOut(config.getTimeout());
                socket.setImmediate(true);
                socket.setReqRelaxed(true);
                socket.setSndHWM(1);
                socket.connect("tcp://"+  key);
                break;
            case DEALER:
                socket = this.context.socket(ZMQ.DEALER);
                // hard code timeout
                socket.setReceiveTimeOut(config.getTimeout());
                socket.setSndHWM(1);
                socket.setImmediate(true);
                socket.setHWM(0);
                socket.connect("tcp://"+  key);
                break;
            case ROUTER:
                socket = this.context.socket(ZMQ.ROUTER);
                socket.setRouterMandatory(true);
                socket.setSendTimeOut(config.getTimeout());
                socket.setReceiveTimeOut(config.getTimeout());
                socket.setHWM(0);
                socket.bind("tcp://"+  key);
                break;
        }

        this.sockets.put(key, socket);

        return socket;
    }

    /**
     * Used by input operators to receive data
     * @param host Host name
     * @param port Port number
     * @return returns received bytes
     */
    public synchronized byte[] receiveSocket(String host, int port) {
        ZMQ.Socket socket = getOrCreateSocket(host, port);
        // non blocking receive needed
        if(socket != null) {
            byte[] message = socket.recv(ZMQ.DONTWAIT);
            checkInSocket(host, port);
            return message;
        }

        return null;
    }

    @Deprecated
    public synchronized int sendSocket(int iteration, ArrayList<Tuple2<String, Integer>> addresses, byte[] message) {
        String currentHost = addresses.get(iteration%addresses.size()).f_0;
        int currentPort = addresses.get(iteration%addresses.size()).f_1;

        ZMQ.Socket socket = SocketPool.getInstance().getOrCreateSocket(currentHost, currentPort);
        String newHost = currentHost;
        int newPort = currentPort;

        for(int i = iteration + 1; !socket.send(message/*, addresses.size() > 1 ? ZMQ.DONTWAIT : 0*/); i++) {
            //socket.close();
            //SocketPool.getInstance().createSocket(SocketPool.SocketType.PUSH, newHost, newPort, this.config);
            iteration = i;
            newHost = addresses.get(i%addresses.size()).f_0;
            newPort = addresses.get(i%addresses.size()).f_1;

            socket = SocketPool.getInstance().getOrCreateSocket(newHost, newPort);
            System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date()).toString() + ": Sending failed. Sending " + SerializationUtils.deserialize(message).toString() + " to " + newHost + ":" + newPort);
        }

        System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date()).toString() + ": Sending " + SerializationUtils.deserialize(message).toString() + " to " + newHost + ":" + newPort + " successful");

        return iteration;
    }

    public synchronized void stopSockets(DSPConnectorConfig config) {
        System.err.println("ERROR: STOPPING SOCKETS");
        for(Tuple2<String, Integer> tuple : config.getAddresses())
            stopSocket(tuple.f_0, tuple.f_1);

        // do not terminate sockets because it could lead to exception when an operator is restarted
        /*if(this.context != null) {
            this.context.term();
            this.context = null;
        }*/
    }

    public synchronized void stopSocket(String host, int port) {
        String key = host + ":" + port;

        if(this.sockets.containsKey(key)) {
            ZMQ.Socket clientSocket = this.sockets.get(key);
            clientSocket.close();
            this.sockets.remove(key);
        }

        if(this.checkedOutSockets.containsKey(key)) {
            ZMQ.Socket clientSocket = this.checkedOutSockets.get(key);
            clientSocket.close();
            this.checkedOutSockets.remove(key);
        }
    }
}
