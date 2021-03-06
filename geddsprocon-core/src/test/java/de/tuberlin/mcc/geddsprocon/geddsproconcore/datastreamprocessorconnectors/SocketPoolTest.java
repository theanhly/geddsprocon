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

package de.tuberlin.mcc.geddsprocon.geddsproconcore.datastreamprocessorconnectors;


import de.tuberlin.mcc.geddsprocon.geddsproconcore.DSPConnectorConfig;
import de.tuberlin.mcc.geddsprocon.geddsproconcore.SocketPool;
import org.junit.Assert;
import org.junit.Test;
import org.zeromq.ZMQ;

public class SocketPoolTest {

    @Test
    public void getSocketTest() {
        try {
            ZMQ.Socket socket = SocketPool.getInstance().getOrCreateSocket(SocketPool.SocketType.PUSH, "localhost", 5555, new DSPConnectorConfig.Builder("localhost", 5555).build());
            ZMQ.Socket socket2 = SocketPool.getInstance().getOrCreateSocket("localhost", 5555);
            ZMQ.Socket socket3 = SocketPool.getInstance().getOrCreateSocket(SocketPool.SocketType.PUSH,"localhost", 5556, new DSPConnectorConfig.Builder("localhost", 5555).build());
            Assert.assertEquals(socket, socket);
            Assert.assertNotEquals(socket, socket3);
            Assert.assertNotEquals(socket2, socket3);
        } catch(IllegalArgumentException ex) {
            System.out.println(ex.toString());
        }
    }
}
