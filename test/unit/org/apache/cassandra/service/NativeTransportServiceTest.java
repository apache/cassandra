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
package org.apache.cassandra.service;

import java.util.Arrays;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.transport.Server;
import org.apache.cassandra.utils.Pair;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class NativeTransportServiceTest
{
    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @After
    public void resetConfig()
    {
        DatabaseDescriptor.getClientEncryptionOptions().enabled = false;
        DatabaseDescriptor.setNativeTransportPortSSL(null);
    }

    @Test
    public void testServiceCanBeStopped()
    {
        withService((NativeTransportService service) -> {
            service.stop();
            assertFalse(service.isRunning());
        });
    }

    @Test
    public void testIgnoresStartOnAlreadyStarted()
    {
        withService((NativeTransportService service) -> {
            service.start();
            service.start();
            service.start();
        });
    }

    @Test
    public void testIgnoresStoppedOnAlreadyStopped()
    {
        withService((NativeTransportService service) -> {
            service.stop();
            service.stop();
            service.stop();
        });
    }

    @Test
    public void testDestroy()
    {
        withService((NativeTransportService service) -> {
            Supplier<Boolean> allTerminated = () ->
                                              service.getWorkerGroup().isShutdown() && service.getWorkerGroup().isTerminated();
            assertFalse(allTerminated.get());
            service.destroy();
            assertTrue(allTerminated.get());
        });
    }

    @Test
    public void testConcurrentStarts()
    {
        withService(NativeTransportService::start, false, 20);
    }

    @Test
    public void testConcurrentStops()
    {
        withService(NativeTransportService::stop, true, 20);
    }

    @Test
    public void testConcurrentDestroys()
    {
        withService(NativeTransportService::destroy, true, 20);
    }

    @Test
    public void testPlainDefaultPort()
    {
        // default plain settings: client encryption disabled and default native transport port 
        withService((NativeTransportService service) ->
                    {
                        assertEquals(1, service.getServers().size());
                        Server server = service.getServers().iterator().next();
                        assertFalse(server.useSSL);
                        assertEquals(server.socket.getPort(), DatabaseDescriptor.getNativeTransportPort());
                    });
    }

    @Test
    public void testSSLOnly()
    {
        // default ssl settings: client encryption enabled and default native transport port used for ssl only
        DatabaseDescriptor.getClientEncryptionOptions().enabled = true;
        DatabaseDescriptor.getClientEncryptionOptions().optional = false;

        withService((NativeTransportService service) ->
                    {
                        service.initialize();
                        assertEquals(1, service.getServers().size());
                        Server server = service.getServers().iterator().next();
                        assertTrue(server.useSSL);
                        assertEquals(server.socket.getPort(), DatabaseDescriptor.getNativeTransportPort());
                    }, false, 1);
    }

    @Test
    public void testSSLOptional()
    {
        // default ssl settings: client encryption enabled and default native transport port used for optional ssl
        DatabaseDescriptor.getClientEncryptionOptions().enabled = true;
        DatabaseDescriptor.getClientEncryptionOptions().optional = true;

        withService((NativeTransportService service) ->
                    {
                        service.initialize();
                        assertEquals(1, service.getServers().size());
                        Server server = service.getServers().iterator().next();
                        assertTrue(server.useSSL);
                        assertEquals(server.socket.getPort(), DatabaseDescriptor.getNativeTransportPort());
                    }, false, 1);
    }

    @Test
    public void testSSLWithNonSSL()
    {
        // ssl+non-ssl settings: client encryption enabled and additional ssl port specified
        DatabaseDescriptor.getClientEncryptionOptions().enabled = true;
        DatabaseDescriptor.setNativeTransportPortSSL(8432);

        withService((NativeTransportService service) ->
                    {
                        service.initialize();
                        assertEquals(2, service.getServers().size());
                        assertEquals(
                                    Sets.newHashSet(Arrays.asList(
                                                                 Pair.create(true, DatabaseDescriptor.getNativeTransportPortSSL()),
                                                                 Pair.create(false, DatabaseDescriptor.getNativeTransportPort())
                                                    )
                                    ),
                                    service.getServers().stream().map((Server s) ->
                                                                      Pair.create(s.useSSL, s.socket.getPort())).collect(Collectors.toSet())
                        );
                    }, false, 1);
    }

    private static void withService(Consumer<NativeTransportService> f)
    {
        withService(f, true, 1);
    }

    private static void withService(Consumer<NativeTransportService> f, boolean start, int concurrently)
    {
        NativeTransportService service = new NativeTransportService();
        assertFalse(service.isRunning());
        if (start)
        {
            service.start();
            assertTrue(service.isRunning());
        }
        try
        {
            if (concurrently == 1)
            {
                f.accept(service);
            }
            else
            {
                IntStream.range(0, concurrently).parallel().map((int i) -> {
                    f.accept(service);
                    return 1;
                }).sum();
            }
        }
        finally
        {
            service.stop();
        }
    }
}
