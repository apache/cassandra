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
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.transport.Server;
import org.apache.cassandra.utils.Pair;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class NativeTransportServiceTest
{
    static EncryptionOptions defaultOptions;

    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
        defaultOptions = DatabaseDescriptor.getNativeProtocolEncryptionOptions();
    }

    @After
    public void resetConfig()
    {
        DatabaseDescriptor.updateNativeProtocolEncryptionOptions(update -> new EncryptionOptions(defaultOptions).applyConfig());
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
            BooleanSupplier allTerminated = () ->
                                            service.getWorkerGroup().isShutdown() && service.getWorkerGroup().isTerminated();
            assertFalse(allTerminated.getAsBoolean());
            service.destroy();
            assertTrue(allTerminated.getAsBoolean());
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
                        assertEquals(EncryptionOptions.TlsEncryptionPolicy.UNENCRYPTED, server.tlsEncryptionPolicy);
                        assertEquals(server.socket.getPort(), DatabaseDescriptor.getNativeTransportPort());
                    });
    }

    @Test
    public void testSSLOnly()
    {
        // default ssl settings: client encryption enabled and default native transport port used for ssl only
        DatabaseDescriptor.updateNativeProtocolEncryptionOptions(options -> options.withEnabled(true)
                                                                                   .withOptional(false));

        withService((NativeTransportService service) ->
                    {
                        service.initialize();
                        assertEquals(1, service.getServers().size());
                        Server server = service.getServers().iterator().next();
                        assertEquals(EncryptionOptions.TlsEncryptionPolicy.ENCRYPTED, server.tlsEncryptionPolicy);
                        assertEquals(server.socket.getPort(), DatabaseDescriptor.getNativeTransportPort());
                    }, false, 1);
    }

    @Test
    public void testSSLOptional()
    {
        // default ssl settings: client encryption enabled and default native transport port used for optional ssl
        DatabaseDescriptor.updateNativeProtocolEncryptionOptions(options -> options.withEnabled(true)
                                                                                   .withOptional(true));

        withService((NativeTransportService service) ->
                    {
                        service.initialize();
                        assertEquals(1, service.getServers().size());
                        Server server = service.getServers().iterator().next();
                        assertEquals(EncryptionOptions.TlsEncryptionPolicy.OPTIONAL, server.tlsEncryptionPolicy);
                        assertEquals(server.socket.getPort(), DatabaseDescriptor.getNativeTransportPort());
                    }, false, 1);
    }

    @Test
    public void testSSLPortWithOptionalEncryption()
    {
        // ssl+non-ssl settings: client encryption enabled and additional ssl port specified
        DatabaseDescriptor.updateNativeProtocolEncryptionOptions(
            options -> options.withEnabled(true)
                              .withOptional(true)
                              .withKeyStore("test/conf/cassandra_ssl_test.keystore"));
        DatabaseDescriptor.setNativeTransportPortSSL(8432);

        withService((NativeTransportService service) ->
                    {
                        service.initialize();
                        assertEquals(2, service.getServers().size());
                        assertEquals(
                                    Sets.newHashSet(Arrays.asList(
                                                                 Pair.create(EncryptionOptions.TlsEncryptionPolicy.OPTIONAL,
                                                                             DatabaseDescriptor.getNativeTransportPortSSL()),
                                                                 Pair.create(EncryptionOptions.TlsEncryptionPolicy.UNENCRYPTED,
                                                                             DatabaseDescriptor.getNativeTransportPort())
                                                    )
                                    ),
                                    service.getServers().stream().map((Server s) ->
                                                                      Pair.create(s.tlsEncryptionPolicy,
                                                                                  s.socket.getPort())).collect(Collectors.toSet())
                        );
                    }, false, 1);
    }

    @Test(expected=java.lang.IllegalStateException.class)
    public void testSSLPortWithDisabledEncryption()
    {
        // ssl+non-ssl settings: client encryption disabled and additional ssl port specified
        // should get an illegal state exception
        DatabaseDescriptor.updateNativeProtocolEncryptionOptions(
        options -> options.withEnabled(false));
        DatabaseDescriptor.setNativeTransportPortSSL(8432);

        withService((NativeTransportService service) ->
                    {
                        service.initialize();
                        assertEquals(1, service.getServers().size());
                        assertEquals(
                        Sets.newHashSet(Arrays.asList(
                        Pair.create(EncryptionOptions.TlsEncryptionPolicy.UNENCRYPTED,
                                    DatabaseDescriptor.getNativeTransportPort())
                                        )
                        ),
                        service.getServers().stream().map((Server s) ->
                                                          Pair.create(s.tlsEncryptionPolicy,
                                                                      s.socket.getPort())).collect(Collectors.toSet())
                        );
                    }, false, 1);
    }

    @Test
    public void testSSLPortWithEnabledSSL()
    {
        // ssl+non-ssl settings: client encryption enabled and additional ssl port specified
        // encryption is enabled and not optional, so listen on both ports requiring encryption
        DatabaseDescriptor.updateNativeProtocolEncryptionOptions(
        options -> options.withEnabled(true)
                          .withOptional(false)
                          .withKeyStore("test/conf/cassandra_ssl_test.keystore"));
        DatabaseDescriptor.setNativeTransportPortSSL(8432);

        withService((NativeTransportService service) ->
                    {
                        service.initialize();
                        assertEquals(2, service.getServers().size());
                        assertEquals(
                        Sets.newHashSet(Arrays.asList(
                        Pair.create(EncryptionOptions.TlsEncryptionPolicy.ENCRYPTED,
                                    DatabaseDescriptor.getNativeTransportPortSSL()),
                        Pair.create(EncryptionOptions.TlsEncryptionPolicy.UNENCRYPTED,
                                    DatabaseDescriptor.getNativeTransportPort())
                                        )
                        ),
                        service.getServers().stream().map((Server s) ->
                                                          Pair.create(s.tlsEncryptionPolicy,
                                                                      s.socket.getPort())).collect(Collectors.toSet())
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
