package de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import de.tuberlin.mcc.geddsprocon.DSPConnectorConfig;
import de.tuberlin.mcc.geddsprocon.tuple.Tuple;
import de.tuberlin.mcc.geddsprocon.tuple.Tuple2;
import org.apache.commons.lang.NullArgumentException;
import org.apache.commons.lang.SerializationUtils;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class SocketPool {
    public enum SocketType { PULL, PUSH, PUB, SUB };

    private static SocketPool socketFactoryInstance = new SocketPool();

    public static SocketPool getInstance() {
        return socketFactoryInstance;
    }


    private ConcurrentHashMap<String, ZMQ.Socket> sockets;
    private ConcurrentHashMap<String, ZMQ.Context> contextMap;
    private ZMQ.Context context;

    private SocketPool() {
        this.sockets = new ConcurrentHashMap<String, ZMQ.Socket>();
        this.contextMap = new ConcurrentHashMap<String, ZMQ.Context>();
        this.context = ZMQ.context(1);
    }

    private static final String IPADDRESS_PATTERN =
            "^([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
                    "([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
                    "([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
                    "([01]?\\d\\d?|2[0-4]\\d|25[0-5])$";

    public synchronized ZMQ.Socket getSocket(String host, int port) throws IllegalArgumentException {
        return getSocket(null, host, port, null);
    }

    public synchronized ZMQ.Socket getSocket(SocketType socketType, String host, int port, DSPConnectorConfig config) throws IllegalArgumentException {

        // TODO: use regex to validate host

        String key = host + ":" + port;
        if(this.sockets.containsKey(key)) {
            ZMQ.Socket clientSocket = this.sockets.get(key);
            return clientSocket;
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
            createSocket(socketType, tuple.f0, tuple.f1, config);
        }
    }

    public synchronized ZMQ.Socket createSocket(SocketType socketType, String host, int port, DSPConnectorConfig config) {
        String key = host + ":" + port;

        ZMQ.Socket socket = null;

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
                socket.connect("tcp://"+  key);
                break;
            case PUB:
                socket = this.context.socket(ZMQ.PUB);
                socket.setSendTimeOut(config.getTimeout());
                socket.setSndHWM(config.getHwm());
                socket.bind("tcp://"+  key);
                break;
        }

        // set hwm
        //socket.setHWM(config.getHwm());

        this.sockets.put(key, socket);
        this.contextMap.put(key, context);

        return socket;
    }

    public synchronized int sendSocket(int iteration, ArrayList<Tuple2<String, Integer>> addresses, byte[] message) {
        String currentHost = addresses.get(iteration%addresses.size()).f0;
        int currentPort = addresses.get(iteration%addresses.size()).f1;

        ZMQ.Socket socket = SocketPool.getInstance().getSocket(currentHost, currentPort);
        String newHost = currentHost;
        int newPort = currentPort;

        for(int i = iteration + 1; !socket.send(message/*, addresses.size() > 1 ? ZMQ.DONTWAIT : 0*/); i++) {
            //socket.close();
            //SocketPool.getInstance().createSocket(SocketPool.SocketType.PUSH, newHost, newPort, this.config);
            iteration = i;
            newHost = addresses.get(i%addresses.size()).f0;
            newPort = addresses.get(i%addresses.size()).f1;

            socket = SocketPool.getInstance().getSocket(newHost, newPort);
            System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date()).toString() + ": Sending failed. Sending " + SerializationUtils.deserialize(message).toString() + " to " + newHost + ":" + newPort);
        }

        System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date()).toString() + ": Sending " + SerializationUtils.deserialize(message).toString() + " to " + newHost + ":" + newPort + " successful");

        return iteration;
    }

    public synchronized boolean sendSocket(String host, int port, byte[] message, DSPConnectorConfig config) {
        ZMQ.Socket socket = getSocket(host, port);
        return socket.send(message);
    }

    public synchronized byte[] receiveSocket(String host, int port) {
        ZMQ.Socket socket = getSocket(host, port);
        // non blocking receive needed
        return socket.recv(ZMQ.DONTWAIT);
    }

}
