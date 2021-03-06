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

package de.tuberlin.mcc.geddsprocon.geddsproconcore.datastreamprocessorconnectors.flinkconnectors;

import de.tuberlin.mcc.geddsprocon.geddsproconcore.DSPConnectorConfig;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.DSPManager;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.common.SerializationTool;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.datastreamprocessorconnectors.IDSPInputOperator;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.SocketPool;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.messagebuffer.IMessageBufferFunction;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.zeromq.ZFrame;
import org.zeromq.ZMsg;

import java.io.Serializable;
import java.util.Iterator;

public class FlinkInputOperator extends RichParallelSourceFunction<Serializable> implements IDSPInputOperator, IMessageBufferFunction {

    private String host;
    private int port;
    private boolean transform;
    private volatile boolean isRunning = true;
    private final DSPConnectorConfig config;
    private final String connectorType;
    private SourceContext<Serializable> ctx;
    private String messageBufferConnectionString;
    private volatile boolean init = false;

    public FlinkInputOperator(DSPConnectorConfig config) {
        this.config = config;
        this.messageBufferConnectionString =  "";

        // create buffer string out of the requester addresses.
        // TODO: move buffer string creation to DSPManager so other DSP operators can access the same method
        if(config.getInputOperatorFaultTolerance()) {
            for(Tuple3<String, Integer, String> tuple : config.getRequestAddresses())
                this.messageBufferConnectionString  += tuple.f_0 + ":" + tuple.f_1 + ";";
        }
        this.host = this.config.getHost();
        this.port = this.config.getPort();
        this.transform = this.config.getTransform();
        this.connectorType = this.config.getConncetorType();
    }

    @Override
    public void open(Configuration parameters) {
        synchronized (DSPManager.getInstance().getDspManagerLock()) {
            DSPManager.getInstance().initiateInputOperator(this.config, this);
            this.init = true;
        }
    }

    @Override
    public void run(SourceContext<Serializable> ctx) {
       collect(ctx);
    }

    private synchronized void collect(SourceContext<Serializable> ctx) {
        while(isRunning && this.init) {
            byte[] byteMessage;

            if(config.getSocketType() == SocketPool.SocketType.PULL) {

                while ((byteMessage = receiveData(this.host, this.port)) != null) {

                    Serializable message = (Serializable)SerializationTool.deserialize(byteMessage);

                    if(message instanceof de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple && this.transform)
                        message = TupleTransformer.transformFromIntermediateTuple((de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple)message);

                    ctx.collect(message);
                }
            } else if(config.getSocketType() == SocketPool.SocketType.REQ || config.getSocketType() == SocketPool.SocketType.DEFAULT) {
                if(!DSPManager.getInstance().getBuffer(this.messageBufferConnectionString, this, config).isEmpty()) {
                    this.ctx = ctx;
                    DSPManager.getInstance().getBuffer(this.messageBufferConnectionString, this, config).flushBuffer(this);
                }
            }
        }
    }

    @Override
    public void cancel() {
        this.isRunning = false;

        try {
            DSPManager.getInstance().stopRequester(this);
            SocketPool.getInstance().stopSockets(this.config);
        } catch (IllegalArgumentException  ex) {
            System.err.println(ex.toString());
        }
    }

    @Override
    @Deprecated
    public byte[] receiveData(String host, int port) {
        return SocketPool.getInstance().receiveSocket(host, port);
    }

    @Override
    public synchronized ZMsg flush(ZMsg messages) {
        if(messages != null) {
            for(ZFrame frame : messages) {

                Serializable message = (Serializable)SerializationTool.deserialize(frame.getData());

                if(message instanceof de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple && this.transform)
                    message = TupleTransformer.transformFromIntermediateTuple((de.tuberlin.mcc.geddsprocon.geddsproconcore.tuple.Tuple)message);

                this.ctx.collect(message);
            }
        }

        return null;
    }
}
