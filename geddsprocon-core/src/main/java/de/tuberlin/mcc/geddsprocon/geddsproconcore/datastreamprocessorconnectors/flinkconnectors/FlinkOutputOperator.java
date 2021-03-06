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

import com.google.common.base.Strings;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.DSPConnectorConfig;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.DSPManager;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.common.SerializationTool;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.datastreamprocessorconnectors.IDSPOutputOperator;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.SocketPool;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.messagebuffer.IMessageBufferFunction;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.messagebuffer.IMessageBufferListener;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.zeromq.ZMsg;

import java.io.Serializable;
import java.util.LinkedList;

public class FlinkOutputOperator extends RichSinkFunction<Serializable> implements IDSPOutputOperator, IMessageBufferFunction {
    private boolean transform;
    private volatile boolean isRunning = true;
    private final DSPConnectorConfig config;
    private String messageBufferConnectionString;
    private volatile boolean init;
    private boolean isMessageBufferProcess;
    private LinkedList outputBuffer;
    private LinkedList previousOutputBuffer;

    public FlinkOutputOperator(DSPConnectorConfig config) {
        // create buffer string out of the requester addresses.
        // TODO: move buffer string creation to DSPManager so other DSP operators can access the same method
        this.messageBufferConnectionString = Strings.isNullOrEmpty(config.getBufferConnectionString()) ? config.getHost() + ":" + config.getPort() : "ipc:///" + config.getBufferConnectionString();
        this.config = config;
        this.transform = config.getTransform();
        this.init = false;
        this.outputBuffer = new LinkedList();
        this.previousOutputBuffer = new LinkedList();
    }

    /**
     * initiate the ouput operator
     * @param parameters
     */
    @Override
    public void open(Configuration parameters) {
        synchronized (DSPManager.getInstance().getDspManagerLock()) {
            DSPManager.getInstance().initiateOutputOperator(this.config, this);
            this.init = true;
            System.out.println("Output Op @Thread-ID: " + Thread.currentThread().getId() + " Init-After: " + this.init);
            this.isMessageBufferProcess = DSPManager.getInstance().getBuffer(this.messageBufferConnectionString).isSeparateBufferProcess();
        }
    }


    /**
     * Called when new data arrives to the sink, and forwards it to Socket.
     *
     * @param value The value to write to the socket.
     */
    @Override
    public void invoke(Serializable value, Context ctx) {
        if(this.isRunning && this.init) {
            if(this.transform && value instanceof Tuple)
                value = TupleTransformer.transformToIntermediateTuple((Tuple)value);

            byte[] byteMessage = SerializationTool.serialize(value);

            // block while the buffer is full
            while(DSPManager.getInstance().getBuffer(this.messageBufferConnectionString).isFull()) {}

            DSPManager.getInstance().getBuffer(this.messageBufferConnectionString).writeBuffer(byteMessage);
        }
    }

    /**
     * Closes the connection with the Socket server.
     */
    @Override
    public void close() {
        this.isRunning = false;
        DSPManager.getInstance().stopRouter(this.config);
        SocketPool.getInstance().stopSockets(this.config);
    }

    @Override
    public IMessageBufferFunction getBufferFunction() {
        return this;
    }

    @Override
    public ZMsg flush(ZMsg message) {
        return message;
    }
}
