package de.tuberlin.mcc.geddsprocon.geddsproconevaluation.common;

import de.tuberlin.mcc.geddsprocon.geddsproconcore.common.SerializationTool;
import org.apache.flink.api.java.tuple.Tuple2;
import org.zeromq.ZMQ;

import java.io.*;
import java.time.LocalDateTime;

public class ZeroMQDataProvider implements Runnable {
    private String host;
    private int port;
    private String file;

    public ZeroMQDataProvider(String host, int port, String file) {
        this.host = host;
        this.port = port;
        this.file = file;
    }

    @Override
    public void run() {
        try {
            ZMQ.Context context = ZMQ.context(1);

            //  Socket to talk to server
            System.out.println("Connecting to hello world server…");

            ZMQ.Socket sender = context.socket(ZMQ.PUSH);
            sender.setSndHWM(100000);
            sender.connect("tcp://" + this.host + ":" + this.port);

            sender.send(SerializationTool.serialize("START_DATA"));
            FileReader reader = new FileReader(this.file);
            BufferedReader tsvReader = new BufferedReader(reader);
            //BufferedWriter writer = new BufferedWriter(new FileWriter("/home/theanhly/Schreibtisch/reviews.txt", true));

            // skip first line
            String newLine = tsvReader.readLine();
            newLine = tsvReader.readLine();
            while(newLine != null && !newLine.isEmpty()) {
                String[] array = newLine.split("\t");
                /*for(String word : array[13].split(" "))
                    sender.send(SerializationTool.serialize(new Tuple2<String, Integer>(word, 1)));*/
                sender.send(SerializationTool.serialize(array[13]));
                //writer.append(array[13] + "\n");
                newLine = tsvReader.readLine();
            }
            System.out.println("ZeroMQDataProvider: Sending END_DATA" );
            sender.send(SerializationTool.serialize("END_DATA"));
            tsvReader.close();
            reader.close();
        } catch (FileNotFoundException ex) {
            System.err.println("File not found");
            System.err.println(ex.toString());
        } catch (IOException ex) {
            System.err.println(ex.toString());
        }
    }
}
