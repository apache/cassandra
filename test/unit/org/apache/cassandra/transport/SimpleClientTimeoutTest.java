/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.transport;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.ServerSocket;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SimpleClientTimeoutTest
{
    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.toolInitialization();
    }

    @Test(timeout = 30000)
    public void testConnectionClosedDetectedWithIndefiniteTimeout() throws Exception
    {
        try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress()))
        {
            Thread closer = new Thread(() -> {
                try
                {
                    server.accept().close();
                }
                catch (IOException e)
                {
                    throw new UncheckedIOException(e);
                }
            });
            closer.start();

            try (SimpleClient client = SimpleClient.builder(server.getInetAddress().getHostAddress(), server.getLocalPort())
                                                   .requestTimeoutSeconds(0)
                                                   .build())
            {
                assertThatThrownBy(() -> client.connect(false))
                .isInstanceOf(SimpleClient.ConnectionClosedException.class);
            }
            closer.join();
        }
    }

    @Test
    public void testConfigurableRequestTimeout() throws Exception
    {
        try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress()))
        {
            try (SimpleClient client = SimpleClient.builder(server.getInetAddress().getHostAddress(), server.getLocalPort())
                                                   .requestTimeoutSeconds(1)
                                                   .build())
            {
                assertThatThrownBy(() -> client.connect(false))
                .isInstanceOf(SimpleClient.TimeoutException.class)
                .hasMessageContaining("1 seconds");
            }
        }
    }
}
