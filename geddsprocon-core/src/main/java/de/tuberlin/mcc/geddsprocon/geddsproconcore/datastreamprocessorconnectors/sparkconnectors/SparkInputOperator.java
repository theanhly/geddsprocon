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

package de.tuberlin.mcc.geddsprocon.geddsproconcore.datastreamprocessorconnectors.sparkconnectors;

import de.tuberlin.mcc.geddsprocon.geddsproconcore.DSPConnectorConfig;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.DSPManager;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.common.SerializationTool;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.datastreamprocessorconnectors.IDSPInputOperator;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.SocketPool;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.messagebuffer.IMessageBufferFunction;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple3;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.zeromq.ZFrame;
import org.zeromq.ZMsg;

import java.io.Serializable;

public class SparkInputOperator extends Receiver<Serializable> implements IDSPInputOperator, IMessageBufferFunction {

    private String host;
    private int port;
    private boolean transform;
    private final DSPConnectorConfig config;
    private String messageBufferConnectionString;
    private volatile boolean init = false;

    public SparkInputOperator(DSPConnectorConfig config) {
        super(StorageLevel.MEMORY_AND_DISK_2());
        this.messageBufferConnectionString = "";

        // create buffer string out of the requester addresses.
        // TODO: move buffer string creation to DSPManager so other DSP operators can access the same method
        if(config.getInputOperatorFaultTolerance()) {
            for(Tuple3<String, Integer, String> tuple : config.getRequestAddresses())
                this.messageBufferConnectionString  += tuple.f_0 + ":" + tuple.f_1 + ";";
        }

        this.config = config;
        this.host = this.config.getHost();
        this.port = this.config.getPort();
        this.transform = this.config.getTransform();
    }

    @Override
    public void onStart() {
        // Start the thread that receives data over a connection
        new Thread(this::startSource).start();
    }

    @Override
    public void onStop() {
        if(isStopped()) {
            SocketPool.getInstance().stopSocket(this.host, this.port);
            DSPManager.getInstance().stopRequester(this);
        }

    }

    public synchronized void startSource() {
        try {
            byte[] byteMessage;

            synchronized (DSPManager.getInstance().getDspManagerLock()) {
                if(!this.init) {
                    DSPManager.getInstance().initiateInputOperator(this.config, this);
                    this.init = true;
                }
            }

            while (!isStopped() && this.init) {
                if(config.getSocketType() == SocketPool.SocketType.PULL) {
                    while((byteMessage = receiveData(this.host, this.port)) != null) {
                        Serializable message = (Serializable)SerializationTool.deserialize(byteMessage);

                        if(message instanceof de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple && transform)
                            message = (Serializable)TupleTransformer.transformFromIntermediateTuple((de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple)message);

                        store(message);
                    }
                } else if (config.getSocketType() == SocketPool.SocketType.REQ || config.getSocketType() == SocketPool.SocketType.DEFAULT) {
                    if(!DSPManager.getInstance().getBuffer(this.messageBufferConnectionString, this, this.config).isEmpty()) {
                        DSPManager.getInstance().getBuffer(this.messageBufferConnectionString, this, this.config).flushBuffer(this);
                    }
                }
            }

        } catch(Throwable t) {
            restart("Error receiving data", t);
        }
    }

    @Override
    public byte[] receiveData(String host, int port) {
        return SocketPool.getInstance().receiveSocket(host, port);
    }

    @Override
    public synchronized ZMsg flush(ZMsg messages) {
        if(messages != null) {
            for(ZFrame frame : messages) {

                Serializable message = (Serializable)SerializationTool.deserialize(frame.getData());

                if(message instanceof de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple && this.transform)
                    message = (Serializable)TupleTransformer.transformFromIntermediateTuple((de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple)message);

                store(message);
            }
        }

        return null;
    }
}
