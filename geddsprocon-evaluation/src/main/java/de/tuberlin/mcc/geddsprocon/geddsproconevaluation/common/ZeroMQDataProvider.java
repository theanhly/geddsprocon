/*
 * Copyright 2019 The-Anh Ly
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.tuberlin.mcc.geddsprocon.geddsproconevaluation.common;

import de.tuberlin.mcc.geddsprocon.geddsproconcore.common.SerializationTool;
import org.apache.flink.api.java.tuple.Tuple2;
import org.zeromq.ZMQ;

import java.io.*;
import java.time.LocalDateTime;

/**
 * Sends the data source via FileReader
 */
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

            ZMQ.Socket sender = context.socket(ZMQ.PUSH);
            sender.setSndHWM(100000);
            sender.connect("tcp://" + this.host + ":" + this.port);

            sender.send(SerializationTool.serialize("START_DATA"));
            FileReader reader = new FileReader(this.file);
            BufferedReader tsvReader = new BufferedReader(reader);

            // skip first line
            String newLine = tsvReader.readLine();
            newLine = tsvReader.readLine();
            while(newLine != null && !newLine.isEmpty()) {
                String[] array = newLine.split("\t");
                sender.send(SerializationTool.serialize(array[13]));
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
