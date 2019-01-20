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

import com.google.common.base.Strings;
import com.typesafe.config.ConfigException;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.DSPConnectorConfig;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.DSPManager;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.common.SerializationTool;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.datastreamprocessorconnectors.IDSPOutputOperator;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.messagebuffer.IMessageBufferFunction;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.function.VoidFunction;
import org.zeromq.ZMsg;

import java.io.Serializable;

public class SparkOutputOperator<T extends JavaRDDLike> implements IDSPOutputOperator, VoidFunction<T>, IMessageBufferFunction {
    private boolean transform;
    private String messageBufferConnectionString;
    private final DSPConnectorConfig config;
    private volatile boolean init = false;

    public SparkOutputOperator(DSPConnectorConfig config) {
        this.config = config;
        this.transform = config.getTransform();

        // create buffer string out of the requester addresses.
        // TODO: move buffer string creation to DSPManager so other DSP operators can access the same method
        this.messageBufferConnectionString = Strings.isNullOrEmpty(config.getBufferConnectionString()) ? this.config.getHost() + ":" + this.config.getPort() : "ipc:///" +  config.getBufferConnectionString();
    }

    @Override
    public void call(T value) throws Exception {
        synchronized (DSPManager.getInstance().getDspManagerLock()) {
            value.foreach(record ->  {

                if(!this.init) {
                    System.out.println("Init Spark output " + java.lang.management.ManagementFactory.getRuntimeMXBean().getName());
                    DSPManager.getInstance().initiateOutputOperator(this.config, this);
                    this.init = true;
                }

                if(this.init) {
                    if (record != null && record instanceof Serializable) {
                        if (record instanceof scala.Product && this.transform)
                            record = TupleTransformer.transformToIntermediateTuple((scala.Product) record);

                        byte[] byteMessage = SerializationTool.serialize((Serializable) record);

                        // block while the buffer is full
                        while (DSPManager.getInstance().getBuffer(this.messageBufferConnectionString).isFull()) {
                        }

                        DSPManager.getInstance().getBuffer(this.messageBufferConnectionString).writeBuffer(byteMessage);
                    }
                }
            });
        }
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
