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

package org.apache.cassandra.net;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.io.IVersionedAsymmetricSerializer;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.proxy.InboundProxyHandler;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.net.ConnectionTest.SETTINGS;
import static org.apache.cassandra.net.OutboundConnectionSettings.Framing.CRC;
import static org.apache.cassandra.utils.MonotonicClock.Global.approxTime;

public class ProxyHandlerConnectionsTest
{
    private static final SocketFactory factory = new SocketFactory();

    private final Map<Verb, Supplier<? extends IVersionedAsymmetricSerializer<?, ?>>> serializers = new HashMap<>();
    private final Map<Verb, Supplier<? extends IVerbHandler<?>>> handlers = new HashMap<>();
    private final Map<Verb, ToLongFunction<TimeUnit>> timeouts = new HashMap<>();

    private void unsafeSetSerializer(Verb verb, Supplier<? extends IVersionedAsymmetricSerializer<?, ?>> supplier) throws Throwable
    {
        serializers.putIfAbsent(verb, verb.unsafeSetSerializer(supplier));
    }

    protected void unsafeSetHandler(Verb verb, Supplier<? extends IVerbHandler<?>> supplier) throws Throwable
    {
        handlers.putIfAbsent(verb, verb.unsafeSetHandler(supplier));
    }

    private void unsafeSetExpiration(Verb verb, ToLongFunction<TimeUnit> expiration) throws Throwable
    {
        timeouts.putIfAbsent(verb, verb.unsafeSetExpiration(expiration));
    }

    @BeforeClass
    public static void startup()
    {
        DatabaseDescriptor.daemonInitialization();
        // call these to initialize everything in case a message is dropped, otherwise we will NPE in the commitlog
        CommitLog.instance.start();
        CompactionManager.instance.getPendingTasks();
    }

    @After
    public void cleanup() throws Throwable
    {
        for (Map.Entry<Verb, Supplier<? extends IVersionedAsymmetricSerializer<?, ?>>> e : serializers.entrySet())
            e.getKey().unsafeSetSerializer(e.getValue());
        serializers.clear();
        for (Map.Entry<Verb, Supplier<? extends IVerbHandler<?>>> e : handlers.entrySet())
            e.getKey().unsafeSetHandler(e.getValue());
        handlers.clear();
        for (Map.Entry<Verb, ToLongFunction<TimeUnit>> e : timeouts.entrySet())
            e.getKey().unsafeSetExpiration(e.getValue());
        timeouts.clear();
    }

    @Test
    public void testExpireInbound() throws Throwable
    {
        DatabaseDescriptor.setCrossNodeTimeout(true);
        testOneManual((settings, inbound, outbound, endpoint, handler) -> {
            unsafeSetSerializer(Verb._TEST_1, FakePayloadSerializer::new);

            CountDownLatch connectionLatch = new CountDownLatch(1);
            unsafeSetHandler(Verb._TEST_1, () -> v -> {
                connectionLatch.countDown();
            });
            outbound.enqueue(Message.out(Verb._TEST_1, 1L));
            connectionLatch.await(10, SECONDS);
            Assert.assertEquals(0, connectionLatch.getCount());

            // Slow things down
            unsafeSetExpiration(Verb._TEST_1, unit -> unit.convert(50, MILLISECONDS));
            handler.withLatency(100, MILLISECONDS);

            unsafeSetHandler(Verb._TEST_1, () -> v -> {
                throw new RuntimeException("Should have not been triggered " + v);
            });
            int expireMessages = 10;
            for (int i = 0; i < expireMessages; i++)
                outbound.enqueue(Message.out(Verb._TEST_1, 1L));

            InboundMessageHandlers handlers = MessagingService.instance().getInbound(endpoint);
            waitForCondition(() -> handlers.expiredCount() == expireMessages);
            Assert.assertEquals(expireMessages, handlers.expiredCount());
        });
    }

    @Test
    public void testExpireSome() throws Throwable
    {
        DatabaseDescriptor.setCrossNodeTimeout(true);
        testOneManual((settings, inbound, outbound, endpoint, handler) -> {
            unsafeSetSerializer(Verb._TEST_1, FakePayloadSerializer::new);
            connect(outbound);

            AtomicInteger counter = new AtomicInteger();
            unsafeSetHandler(Verb._TEST_1, () -> v -> {
                counter.incrementAndGet();
            });

            int expireMessages = 10;
            for (int i = 0; i < expireMessages; i++)
                outbound.enqueue(Message.out(Verb._TEST_1, 1L));
            waitForCondition(() -> counter.get() == 10);

            unsafeSetExpiration(Verb._TEST_1, unit -> unit.convert(50, MILLISECONDS));
            handler.withLatency(100, MILLISECONDS);

            InboundMessageHandlers handlers = MessagingService.instance().getInbound(endpoint);
            for (int i = 0; i < expireMessages; i++)
                outbound.enqueue(Message.out(Verb._TEST_1, 1L));
            waitForCondition(() -> handlers.expiredCount() == 10);

            handler.withLatency(2, MILLISECONDS);

            for (int i = 0; i < expireMessages; i++)
                outbound.enqueue(Message.out(Verb._TEST_1, 1L));
            waitForCondition(() -> counter.get() == 20);
        });
    }

    @Test
    public void testExpireSomeFromBatch() throws Throwable
    {
        DatabaseDescriptor.setCrossNodeTimeout(true);
        testManual((settings, inbound, outbound, endpoint, handler) -> {
            unsafeSetSerializer(Verb._TEST_1, FakePayloadSerializer::new);
            connect(outbound);

            Message msg = Message.out(Verb._TEST_1, 1L);
            int messageSize = msg.serializedSize(MessagingService.current_version);
            DatabaseDescriptor.setInternodeMaxMessageSizeInBytes(messageSize * 40);

            AtomicInteger counter = new AtomicInteger();
            unsafeSetHandler(Verb._TEST_1, () -> v -> {
                counter.incrementAndGet();
            });

            // use much large timeout here since this is temporally sensitive and could fail on slower/busy machines
            unsafeSetExpiration(Verb._TEST_1, unit -> unit.convert(2000, MILLISECONDS));
            handler.withLatency(1000, MILLISECONDS);

            int expireMessages = 20;
            CountDownLatch enqueueDone = new CountDownLatch(1);
            outbound.unsafeRunOnDelivery(() -> Uninterruptibles.awaitUninterruptibly(enqueueDone, 10, SECONDS));

            for (int i = 0; i < expireMessages; i++)
            {
                long nanoTime = approxTime.now();
                boolean expire = i % 2 == 0;
                Message.Builder builder = Message.builder(Verb._TEST_1, 1L);

                // Give messages 500 milliseconds to leave outbound path
                builder.withCreatedAt(nanoTime)
                       .withExpiresAt(nanoTime + (expire ? MILLISECONDS.toNanos(500) : MILLISECONDS.toNanos(3000)));

                    outbound.enqueue(builder.build());
            }
            enqueueDone.countDown();

            InboundMessageHandlers handlers = MessagingService.instance().getInbound(endpoint);
            waitForCondition(() -> handlers.expiredCount() == 10 && counter.get() == 10,
                             () -> String.format("Expired: %d, Arrived: %d", handlers.expiredCount(), counter.get()));
        });
    }

    @Test
    public void suddenDisconnect() throws Throwable
    {
        testManual((settings, inbound, outbound, endpoint, handler) -> {
            handler.onDisconnect(() -> handler.reset());

            unsafeSetSerializer(Verb._TEST_1, FakePayloadSerializer::new);
            connect(outbound);

            CountDownLatch closeLatch = new CountDownLatch(1);
            handler.withCloseAfterRead(closeLatch::countDown);
            AtomicInteger counter = new AtomicInteger();
            unsafeSetHandler(Verb._TEST_1, () -> v -> counter.incrementAndGet());

            outbound.enqueue(Message.out(Verb._TEST_1, 1L));
            waitForCondition(() -> !outbound.isConnected());

            connect(outbound);
            Assert.assertTrue(outbound.isConnected());
            Assert.assertEquals(0, counter.get());
        });
    }

    @Test
    public void testCorruptionOnHandshake() throws Throwable
    {
        testManual((settings, inbound, outbound, endpoint, handler) -> {
            unsafeSetSerializer(Verb._TEST_1, FakePayloadSerializer::new);
            // Invalid CRC
            handler.withPayloadTransform(msg -> {
                ByteBuf bb = (ByteBuf) msg;
                bb.setByte(bb.readableBytes() / 2, 0xffff);
                return msg;
            });
            tryConnect(outbound, 1, SECONDS, false);
            Assert.assertTrue(!outbound.isConnected());

            // Invalid protocol magic
            handler.withPayloadTransform(msg -> {
                ByteBuf bb = (ByteBuf) msg;
                bb.setByte(0, 0xffff);
                return msg;
            });
            tryConnect(outbound, 1, SECONDS, false);
            Assert.assertTrue(!outbound.isConnected());
            if (settings.right.framing == CRC)
            {
                Assert.assertEquals(2, outbound.connectionAttempts());
                Assert.assertEquals(0, outbound.successfulConnections());
            }
        });
    }

    private static void waitForCondition(Supplier<Boolean> cond) throws Throwable
    {
        CompletableFuture.runAsync(() -> {
            while (!cond.get()) {}
        }).get(1, MINUTES);
    }

    private static void waitForCondition(Supplier<Boolean> cond, Supplier<String> s) throws Throwable
    {
        try
        {
            CompletableFuture.runAsync(() -> {
                while (!cond.get()) {}
            }).get(30, SECONDS);
        }
        catch (TimeoutException e)
        {
            throw new AssertionError(s.get());
        }
    }

    private static class FakePayloadSerializer implements IVersionedSerializer<Long>
    {
        private final int size;
        private FakePayloadSerializer()
        {
            this(1);
        }

        // Takes long and repeats it size times
        private FakePayloadSerializer(int size)
        {
            this.size = size;
        }

        public void serialize(Long i, DataOutputPlus out, int version) throws IOException
        {
            for (int j = 0; j < size; j++)
            {
                out.writeLong(i);
            }
        }

        public Long deserialize(DataInputPlus in, int version) throws IOException
        {
            long l = in.readLong();
            for (int i = 0; i < size - 1; i++)
            {
                if (in.readLong() != l)
                    throw new AssertionError();
            }

            return l;
        }

        public long serializedSize(Long t, int version)
        {
            return Long.BYTES * size;
        }
    }
    interface ManualSendTest
    {
        void accept(Pair<InboundConnectionSettings, OutboundConnectionSettings> settings, InboundSockets inbound, OutboundConnection outbound, InetAddressAndPort endpoint, InboundProxyHandler.Controller handler) throws Throwable;
    }

    private void testManual(ManualSendTest test) throws Throwable
    {
        for (ConnectionTest.Settings s: SETTINGS)
        {
            doTestManual(s, test);
            cleanup();
        }
    }

    private void testOneManual(ManualSendTest test) throws Throwable
    {
        testOneManual(test, 1);
    }

    private void testOneManual(ManualSendTest test, int i) throws Throwable
    {
        ConnectionTest.Settings s = SETTINGS.get(i);
        doTestManual(s, test);
        cleanup();
    }

    private void doTestManual(ConnectionTest.Settings settings, ManualSendTest test) throws Throwable
    {
        InetAddressAndPort endpoint = FBUtilities.getBroadcastAddressAndPort();

        InboundConnectionSettings inboundSettings = settings.inbound.apply(new InboundConnectionSettings())
                                                                    .withBindAddress(endpoint)
                                                                    .withSocketFactory(factory);

        InboundSockets inbound = new InboundSockets(Collections.singletonList(inboundSettings));

        OutboundConnectionSettings outboundSettings = settings.outbound.apply(new OutboundConnectionSettings(endpoint))
                                                                       .withConnectTo(endpoint)
                                                                       .withDefaultReserveLimits()
                                                                       .withSocketFactory(factory);

        ResourceLimits.EndpointAndGlobal reserveCapacityInBytes = new ResourceLimits.EndpointAndGlobal(new ResourceLimits.Concurrent(outboundSettings.applicationSendQueueReserveEndpointCapacityInBytes), outboundSettings.applicationSendQueueReserveGlobalCapacityInBytes);
        OutboundConnection outbound = new OutboundConnection(settings.type, outboundSettings, reserveCapacityInBytes);
        try
        {
            InboundProxyHandler.Controller controller = new InboundProxyHandler.Controller();
            inbound.open(pipeline -> {
                InboundProxyHandler handler = new InboundProxyHandler(controller);
                pipeline.addLast(handler);
            }).sync();
            test.accept(Pair.create(inboundSettings, outboundSettings), inbound, outbound, endpoint, controller);
        }
        finally
        {
            outbound.close(false);
            inbound.close().get(30L, SECONDS);
            outbound.close(false).get(30L, SECONDS);
            MessagingService.instance().messageHandlers.clear();
        }
    }

    private void connect(OutboundConnection outbound) throws Throwable
    {
        tryConnect(outbound, 10, SECONDS, true);
    }

    private void tryConnect(OutboundConnection outbound, long timeout, TimeUnit timeUnit, boolean throwOnFailure) throws Throwable
    {
        CountDownLatch connectionLatch = new CountDownLatch(1);
        unsafeSetHandler(Verb._TEST_1, () -> v -> {
            connectionLatch.countDown();
        });
        outbound.enqueue(Message.out(Verb._TEST_1, 1L));
        connectionLatch.await(timeout, timeUnit);
        if (throwOnFailure)
            Assert.assertEquals(0, connectionLatch.getCount());
    }
}
