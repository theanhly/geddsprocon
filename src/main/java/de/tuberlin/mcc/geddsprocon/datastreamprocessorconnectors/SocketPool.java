package de.tuberlin.mcc.geddsprocon.datastreamprocessorconnectors;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class SocketPool {
    public enum SocketType { PULL, PUSH };

    private static SocketPool socketFactoryInstance = new SocketPool();

    public static SocketPool getInstance() {
        return socketFactoryInstance;
    }


    private ConcurrentHashMap<String, ZMQ.Socket> sockets;
    private ConcurrentHashMap<String, ZMQ.Context> contextMap;

    private SocketPool() {
        this.sockets = new ConcurrentHashMap<String, ZMQ.Socket>();
        this.contextMap = new ConcurrentHashMap<String, ZMQ.Context>();
    }

    private static final String IPADDRESS_PATTERN =
            "^([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
                    "([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
                    "([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
                    "([01]?\\d\\d?|2[0-4]\\d|25[0-5])$";

    public synchronized ZMQ.Socket getSocket(String host, int port) throws IllegalArgumentException {
        return getSocket(null, host, port);
    }

    public synchronized ZMQ.Socket getSocket(SocketType socketType, String host, int port) throws IllegalArgumentException {

        return getSocket(socketType, host, port, 0);
    }

    public synchronized ZMQ.Socket getSocket(SocketType socketType, String host, int port, int setHWM) throws IllegalArgumentException {

        // TODO: use regex to validate host

        String key = host + ":" + port;
        if(this.sockets.containsKey(key)) {
            ZMQ.Socket clientSocket = this.sockets.get(key);
            return clientSocket;
        } else {

            ZMQ.Context context = ZMQ.context(1);

            ZMQ.Socket socket = null;

            if (socketType == null)
                throw new IllegalArgumentException(String.format("Socket with host {0} and port {1} not found. Parameter socketType needs to be defined.", host, port));

            switch(socketType) {
                case PULL:
                    socket = context.socket(ZMQ.PULL);
                    socket.bind("tcp://"+  key);
                    break;
                case PUSH:
                    socket = context.socket(ZMQ.PUSH);
                    socket.connect("tcp://"+  key);
                    break;
            }

            // set hwm
            socket.setHWM(setHWM);

            this.sockets.put(key, socket);
            this.contextMap.put(key, context);

            return socket;
        }
    }

    public synchronized void checkinSocket(String host, int port) {
        //String key = host + ":" + port;
        //ZMQ.Socket clientSocket = this.sockets.get(key);
        //this.lockedClientSockets.remove(key);
        //this.freeClientSockets.put(key, clientSocket);
    }

    public synchronized void sendSocket(String host, int port, byte[] message) throws InterruptedException{
        ZMQ.Socket socket = getSocket(host, port);
        socket.send(message);
    }

    public synchronized byte[] receiveSocket(String host, int port) throws InterruptedException {
        ZMQ.Socket socket = getSocket(host, port);
        byte[] byteMessage = socket.recv(ZMQ.NOBLOCK);
        return byteMessage;
    }

}
