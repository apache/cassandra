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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.exceptions.UnknownColumnException;
import org.apache.cassandra.io.IVersionedAsymmetricSerializer;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.FBUtilities;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.net.MessagingService.VERSION_40;
import static org.apache.cassandra.net.NoPayload.noPayload;
import static org.apache.cassandra.net.MessagingService.current_version;
import static org.apache.cassandra.net.ConnectionType.LARGE_MESSAGES;
import static org.apache.cassandra.net.ConnectionType.SMALL_MESSAGES;
import static org.apache.cassandra.net.ConnectionUtils.*;
import static org.apache.cassandra.net.OutboundConnectionSettings.Framing.LZ4;
import static org.apache.cassandra.net.OutboundConnections.LARGE_MESSAGE_THRESHOLD;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.apache.cassandra.utils.MonotonicClock.Global.approxTime;

public class ConnectionTest
{
    private static final Logger logger = LoggerFactory.getLogger(ConnectionTest.class);
    private static final SocketFactory factory = new SocketFactory();

    private final Map<Verb, Supplier<? extends IVersionedAsymmetricSerializer<?, ?>>> serializers = new HashMap<>();
    private final Map<Verb, Supplier<? extends IVerbHandler<?>>> handlers = new HashMap<>();
    private final Map<Verb, ToLongFunction<TimeUnit>> timeouts = new HashMap<>();

    private void unsafeSetSerializer(Verb verb, Supplier<? extends IVersionedAsymmetricSerializer<?, ?>> supplier) throws Throwable
    {
        serializers.putIfAbsent(verb, verb.unsafeSetSerializer(supplier));
    }

    private void unsafeSetHandler(Verb verb, Supplier<? extends IVerbHandler<?>> supplier) throws Throwable
    {
        handlers.putIfAbsent(verb, verb.unsafeSetHandler(supplier));
    }

    @After
    public void resetVerbs() throws Throwable
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

    @BeforeClass
    public static void startup()
    {
        DatabaseDescriptor.daemonInitialization();
        CommitLog.instance.start();
    }

    @AfterClass
    public static void cleanup() throws InterruptedException
    {
        factory.shutdownNow();
    }

    interface SendTest
    {
        void accept(InboundMessageHandlers inbound, OutboundConnection outbound, InetAddressAndPort endpoint) throws Throwable;
    }

    interface ManualSendTest
    {
        void accept(Settings settings, InboundSockets inbound, OutboundConnection outbound, InetAddressAndPort endpoint) throws Throwable;
    }

    static class Settings
    {
        static final Settings SMALL = new Settings(SMALL_MESSAGES);
        static final Settings LARGE = new Settings(LARGE_MESSAGES);
        final ConnectionType type;
        final Function<OutboundConnectionSettings, OutboundConnectionSettings> outbound;
        final Function<InboundConnectionSettings, InboundConnectionSettings> inbound;
        Settings(ConnectionType type)
        {
            this(type, Function.identity(), Function.identity());
        }
        Settings(ConnectionType type, Function<OutboundConnectionSettings, OutboundConnectionSettings> outbound,
                 Function<InboundConnectionSettings, InboundConnectionSettings> inbound)
        {
            this.type = type;
            this.outbound = outbound;
            this.inbound = inbound;
        }
        Settings outbound(Function<OutboundConnectionSettings, OutboundConnectionSettings> outbound)
        {
            return new Settings(type, this.outbound.andThen(outbound), inbound);
        }
        Settings inbound(Function<InboundConnectionSettings, InboundConnectionSettings> inbound)
        {
            return new Settings(type, outbound, this.inbound.andThen(inbound));
        }
        Settings override(Settings settings)
        {
            return new Settings(settings.type != null ? settings.type : type,
                                outbound.andThen(settings.outbound),
                                inbound.andThen(settings.inbound));
        }
    }

    static final EncryptionOptions.ServerEncryptionOptions encryptionOptions =
            new EncryptionOptions.ServerEncryptionOptions()
            .withLegacySslStoragePort(true)
            .withOptional(true)
            .withInternodeEncryption(EncryptionOptions.ServerEncryptionOptions.InternodeEncryption.all)
            .withKeyStore("test/conf/cassandra_ssl_test.keystore")
            .withKeyStorePassword("cassandra")
            .withTrustStore("test/conf/cassandra_ssl_test.truststore")
            .withTrustStorePassword("cassandra")
            .withRequireClientAuth(false)
            .withCipherSuites("TLS_RSA_WITH_AES_128_CBC_SHA");

    static final List<Function<Settings, Settings>> MODIFIERS = ImmutableList.of(
        settings -> settings.outbound(outbound -> outbound.withEncryption(encryptionOptions))
                            .inbound(inbound -> inbound.withEncryption(encryptionOptions)),
        settings -> settings.outbound(outbound -> outbound.withFraming(LZ4))
    );

    static final List<Settings> SETTINGS = applyPowerSet(
        ImmutableList.of(Settings.SMALL, Settings.LARGE),
        MODIFIERS
    );

    private static <T> List<T> applyPowerSet(List<T> settings, List<Function<T, T>> modifiers)
    {
        List<T> result = new ArrayList<>();
        for (Set<Function<T, T>> set : Sets.powerSet(new HashSet<>(modifiers)))
        {
            for (T s : settings)
            {
                for (Function<T, T> f : set)
                    s = f.apply(s);
                result.add(s);
            }
        }
        return result;
    }

    private void test(Settings extraSettings, SendTest test) throws Throwable
    {
        for (Settings s : SETTINGS)
            doTest(s.override(extraSettings), test);
    }
    private void test(SendTest test) throws Throwable
    {
        for (Settings s : SETTINGS)
            doTest(s, test);
    }

    private void testManual(ManualSendTest test) throws Throwable
    {
        for (Settings s : SETTINGS)
            doTestManual(s, test);
    }

    private void doTest(Settings settings, SendTest test) throws Throwable
    {
        doTestManual(settings, (ignore, inbound, outbound, endpoint) -> {
            inbound.open().sync();
            test.accept(MessagingService.instance().getInbound(endpoint), outbound, endpoint);
        });
    }

    private void doTestManual(Settings settings, ManualSendTest test) throws Throwable
    {
        InetAddressAndPort endpoint = FBUtilities.getBroadcastAddressAndPort();
        InboundConnectionSettings inboundSettings = settings.inbound.apply(new InboundConnectionSettings())
                                                                    .withBindAddress(endpoint)
                                                                    .withSocketFactory(factory);
        InboundSockets inbound = new InboundSockets(Collections.singletonList(inboundSettings));
        OutboundConnectionSettings outboundTemplate = settings.outbound.apply(new OutboundConnectionSettings(endpoint))
                                                                       .withDefaultReserveLimits()
                                                                       .withSocketFactory(factory)
                                                                       .withDefaults(ConnectionCategory.MESSAGING);
        ResourceLimits.EndpointAndGlobal reserveCapacityInBytes = new ResourceLimits.EndpointAndGlobal(new ResourceLimits.Concurrent(outboundTemplate.applicationSendQueueReserveEndpointCapacityInBytes), outboundTemplate.applicationSendQueueReserveGlobalCapacityInBytes);
        OutboundConnection outbound = new OutboundConnection(settings.type, outboundTemplate, reserveCapacityInBytes);
        try
        {
            logger.info("Running {} {} -> {}", outbound.messagingVersion(), outbound.settings(), inboundSettings);
            test.accept(settings, inbound, outbound, endpoint);
        }
        finally
        {
            outbound.close(false);
            inbound.close().get(30L, SECONDS);
            outbound.close(false).get(30L, SECONDS);
            resetVerbs();
            MessagingService.instance().messageHandlers.clear();
        }
    }

    @Test
    public void testSendSmall() throws Throwable
    {
        test((inbound, outbound, endpoint) -> {
            int version = outbound.settings().acceptVersions.max;
            int count = 10;

            CountDownLatch deliveryDone = new CountDownLatch(1);
            CountDownLatch receiveDone = new CountDownLatch(count);

            unsafeSetHandler(Verb._TEST_1, () -> msg -> receiveDone.countDown());
            Message<?> message = Message.out(Verb._TEST_1, noPayload);
            for (int i = 0 ; i < count ; ++i)
                outbound.enqueue(message);

            Assert.assertTrue(receiveDone.await(10, SECONDS));
            outbound.unsafeRunOnDelivery(deliveryDone::countDown);
            Assert.assertTrue(deliveryDone.await(10, SECONDS));

            check(outbound).submitted(10)
                           .sent     (10, 10 * message.serializedSize(version))
                           .pending  ( 0,  0)
                           .overload ( 0,  0)
                           .expired  ( 0,  0)
                           .error    ( 0,  0)
                           .check();
            check(inbound) .received (10, 10 * message.serializedSize(version))
                           .processed(10, 10 * message.serializedSize(version))
                           .pending  ( 0,  0)
                           .expired  ( 0,  0)
                           .error    ( 0,  0)
                           .check();
        });
    }

    @Test
    public void testSendLarge() throws Throwable
    {
        test((inbound, outbound, endpoint) -> {
            int version = outbound.settings().acceptVersions.max;
            int count = 10;

            CountDownLatch deliveryDone = new CountDownLatch(1);
            CountDownLatch receiveDone = new CountDownLatch(count);

            unsafeSetSerializer(Verb._TEST_1, () -> new IVersionedSerializer<Object>()
            {
                public void serialize(Object noPayload, DataOutputPlus out, int version) throws IOException
                {
                    for (int i = 0 ; i < LARGE_MESSAGE_THRESHOLD + 1 ; ++i)
                        out.writeByte(i);
                }
                public Object deserialize(DataInputPlus in, int version) throws IOException
                {
                    in.skipBytesFully(LARGE_MESSAGE_THRESHOLD + 1);
                    return noPayload;
                }
                public long serializedSize(Object noPayload, int version)
                {
                    return LARGE_MESSAGE_THRESHOLD + 1;
                }
            });
            unsafeSetHandler(Verb._TEST_1, () -> msg -> receiveDone.countDown());
            Message<?> message = Message.builder(Verb._TEST_1, new Object())
                                        .withExpiresAt(nanoTime() + SECONDS.toNanos(30L))
                                        .build();
            for (int i = 0 ; i < count ; ++i)
                outbound.enqueue(message);
            Assert.assertTrue(receiveDone.await(10, SECONDS));

            outbound.unsafeRunOnDelivery(deliveryDone::countDown);
            Assert.assertTrue(deliveryDone.await(10, SECONDS));

            check(outbound).submitted(10)
                           .sent     (10, 10 * message.serializedSize(version))
                           .pending  ( 0,  0)
                           .overload ( 0,  0)
                           .expired  ( 0,  0)
                           .error    ( 0,  0)
                           .check();
            check(inbound) .received (10, 10 * message.serializedSize(version))
                           .processed(10, 10 * message.serializedSize(version))
                           .pending  ( 0,  0)
                           .expired  ( 0,  0)
                           .error    ( 0,  0)
                           .check();
        });
    }

    @Test
    public void testInsufficientSpace() throws Throwable
    {
        test(new Settings(null).outbound(settings -> settings
                                         .withApplicationReserveSendQueueCapacityInBytes(1 << 15, new ResourceLimits.Concurrent(1 << 16))
                                         .withApplicationSendQueueCapacityInBytes(1 << 16)),
             (inbound, outbound, endpoint) -> {

            CountDownLatch done = new CountDownLatch(1);
            Message<?> message = Message.out(Verb._TEST_1, new Object());
            MessagingService.instance().callbacks.addWithExpiration(new RequestCallback()
            {
                @Override
                public void onFailure(InetAddressAndPort from, RequestFailureReason failureReason)
                {
                    done.countDown();
                }

                @Override
                public boolean invokeOnFailure()
                {
                    return true;
                }

                @Override
                public void onResponse(Message msg)
                {
                    throw new IllegalStateException();
                }

            }, message, endpoint);
            AtomicInteger delivered = new AtomicInteger();
            unsafeSetSerializer(Verb._TEST_1, () -> new IVersionedSerializer<Object>()
            {
                public void serialize(Object o, DataOutputPlus out, int version) throws IOException
                {
                    for (int i = 0 ; i <= 4 << 16 ; i += 8L)
                        out.writeLong(1L);
                }

                public Object deserialize(DataInputPlus in, int version) throws IOException
                {
                    in.skipBytesFully(4 << 16);
                    return null;
                }

                public long serializedSize(Object o, int version)
                {
                    return 4 << 16;
                }
            });
            unsafeSetHandler(Verb._TEST_1, () -> msg -> delivered.incrementAndGet());
            outbound.enqueue(message);
            Assert.assertTrue(done.await(10, SECONDS));
            Assert.assertEquals(0, delivered.get());
                 check(outbound).submitted( 1)
                                .sent     ( 0,  0)
                                .pending  ( 0,  0)
                                .overload ( 1,  message.serializedSize(current_version))
                                .expired  ( 0,  0)
                                .error    ( 0,  0)
                                .check();
                 check(inbound) .received ( 0,  0)
                                .processed( 0,  0)
                                .pending  ( 0,  0)
                                .expired  ( 0,  0)
                                .error    ( 0,  0)
                                .check();
        });
    }

    @Test
    public void testSerializeError() throws Throwable
    {
        test((inbound, outbound, endpoint) -> {
            int version = outbound.settings().acceptVersions.max;
            int count = 100;

            CountDownLatch deliveryDone = new CountDownLatch(1);
            CountDownLatch receiveDone = new CountDownLatch(90);

            AtomicInteger serialized = new AtomicInteger();
            Message<?> message = Message.builder(Verb._TEST_1, new Object())
                                        .withExpiresAt(nanoTime() + SECONDS.toNanos(30L))
                                        .build();
            unsafeSetSerializer(Verb._TEST_1, () -> new IVersionedSerializer<Object>()
            {
                public void serialize(Object o, DataOutputPlus out, int version) throws IOException
                {
                    int i = serialized.incrementAndGet();
                    if (0 == (i & 15))
                    {
                        if (0 == (i & 16))
                            out.writeByte(i);
                        throw new IOException();
                    }

                    if (1 != (i & 31))
                        out.writeByte(i);
                }

                public Object deserialize(DataInputPlus in, int version) throws IOException
                {
                    in.readByte();
                    return null;
                }

                public long serializedSize(Object o, int version)
                {
                    return 1;
                }
            });

            unsafeSetHandler(Verb._TEST_1, () -> msg -> receiveDone.countDown());
            for (int i = 0 ; i < count ; ++i)
                outbound.enqueue(message);

            Assert.assertTrue(receiveDone.await(1, MINUTES));
            outbound.unsafeRunOnDelivery(deliveryDone::countDown);
            Assert.assertTrue(deliveryDone.await(10, SECONDS));

            check(outbound).submitted(100)
                           .sent     ( 90, 90 * message.serializedSize(version))
                           .pending  (  0,  0)
                           .overload (  0,  0)
                           .expired  (  0,  0)
                           .error    ( 10, 10 * message.serializedSize(version))
                           .check();
            check(inbound) .received ( 90, 90 * message.serializedSize(version))
                           .processed( 90, 90 * message.serializedSize(version))
                           .pending  (  0,  0)
                           .expired  (  0,  0)
                           .error    (  0,  0)
                           .check();
        });
    }

    @Test
    public void testTimeout() throws Throwable
    {
        test((inbound, outbound, endpoint) -> {
            int version = outbound.settings().acceptVersions.max;
            int count = 10;
            CountDownLatch enqueueDone = new CountDownLatch(1);
            CountDownLatch deliveryDone = new CountDownLatch(1);
            AtomicInteger delivered = new AtomicInteger();
            Verb._TEST_1.unsafeSetHandler(() -> msg -> delivered.incrementAndGet());
            Message<?> message = Message.builder(Verb._TEST_1, noPayload)
                                        .withExpiresAt(approxTime.now() + TimeUnit.DAYS.toNanos(1L))
                                        .build();
            long sentSize = message.serializedSize(version);
            outbound.enqueue(message);
            long timeoutMillis = 10L;
            while (delivered.get() < 1);
            outbound.unsafeRunOnDelivery(() -> Uninterruptibles.awaitUninterruptibly(enqueueDone, 1L, TimeUnit.DAYS));
            message = Message.builder(Verb._TEST_1, noPayload)
                             .withExpiresAt(approxTime.now() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis))
                             .build();
            for (int i = 0 ; i < count ; ++i)
                outbound.enqueue(message);
            Uninterruptibles.sleepUninterruptibly(timeoutMillis * 2, TimeUnit.MILLISECONDS);
            enqueueDone.countDown();
            outbound.unsafeRunOnDelivery(deliveryDone::countDown);
            Assert.assertTrue(deliveryDone.await(1, MINUTES));
            Assert.assertEquals(1, delivered.get());
            check(outbound).submitted( 11)
                           .sent     (  1,  sentSize)
                           .pending  (  0,  0)
                           .overload (  0,  0)
                           .expired  ( 10, 10 * message.serializedSize(current_version))
                           .error    (  0,  0)
                           .check();
            check(inbound) .received (  1, sentSize)
                           .processed(  1, sentSize)
                           .pending  (  0,  0)
                           .expired  (  0,  0)
                           .error    (  0,  0)
                           .check();
        });
    }

    @Test
    public void testCloseIfEndpointDown() throws Throwable
    {
        testManual((settings, inbound, outbound, endpoint) -> {
            Message<?> message = Message.builder(Verb._TEST_1, noPayload)
                                        .withExpiresAt(nanoTime() + SECONDS.toNanos(30L))
                                        .build();

            for (int i = 0 ; i < 1000 ; ++i)
                outbound.enqueue(message);

            outbound.close(true).get(10L, MINUTES);
        });
    }

    @Test
    public void testMessagePurging() throws Throwable
    {
        testManual((settings, inbound, outbound, endpoint) -> {
            Runnable testWhileDisconnected = () -> {
                try
                {
                    for (int i = 0; i < 5; i++)
                    {
                        Message<?> message = Message.builder(Verb._TEST_1, noPayload)
                                                    .withExpiresAt(nanoTime() + MILLISECONDS.toNanos(50L))
                                                    .build();
                        OutboundMessageQueue queue = outbound.queue;
                        while (true)
                        {
                            try (OutboundMessageQueue.WithLock withLock = queue.lockOrCallback(nanoTime(), null))
                            {
                                if (withLock != null)
                                {
                                    outbound.enqueue(message);
                                    Assert.assertFalse(outbound.isConnected());
                                    Assert.assertEquals(1, outbound.pendingCount());
                                    break;
                                }
                            }
                        }

                        CompletableFuture.runAsync(() -> {
                            while (outbound.pendingCount() > 0 && !Thread.interrupted()) {}
                        }).get(10, SECONDS);
                        // Message should have been purged
                        Assert.assertEquals(0, outbound.pendingCount());
                    }
                }
                catch (Throwable t)
                {
                    throw new RuntimeException(t);
                }
            };

            testWhileDisconnected.run();

            try
            {
                inbound.open().sync();
                CountDownLatch deliveryDone = new CountDownLatch(1);

                unsafeSetHandler(Verb._TEST_1, () -> msg -> {
                    outbound.unsafeRunOnDelivery(deliveryDone::countDown);
                });
                outbound.enqueue(Message.out(Verb._TEST_1, noPayload));
                Assert.assertEquals(1, outbound.pendingCount());

                Assert.assertTrue(deliveryDone.await(10, SECONDS));
                Assert.assertEquals(0, deliveryDone.getCount());
                Assert.assertEquals(0, outbound.pendingCount());
            }
            finally
            {
                inbound.close().get(10, SECONDS);
                // Wait until disconnected
                CompletableFuture.runAsync(() -> {
                    while (outbound.isConnected() && !Thread.interrupted()) {}
                }).get(10, SECONDS);
            }

            testWhileDisconnected.run();
        });
    }

    @Test
    public void testMessageDeliveryOnReconnect() throws Throwable
    {
        testManual((settings, inbound, outbound, endpoint) -> {
            try
            {
                inbound.open().sync();
                CountDownLatch done = new CountDownLatch(1);
                unsafeSetHandler(Verb._TEST_1, () -> msg -> done.countDown());
                outbound.enqueue(Message.out(Verb._TEST_1, noPayload));
                Assert.assertTrue(done.await(10, SECONDS));
                Assert.assertEquals(0, done.getCount());

                // Simulate disconnect
                inbound.close().get(10, SECONDS);
                MessagingService.instance().removeInbound(endpoint);
                inbound = new InboundSockets(settings.inbound.apply(new InboundConnectionSettings()));
                inbound.open().sync();

                CountDownLatch latch2 = new CountDownLatch(1);
                unsafeSetHandler(Verb._TEST_1, () -> msg -> latch2.countDown());
                outbound.enqueue(Message.out(Verb._TEST_1, noPayload));

                latch2.await(10, SECONDS);
                Assert.assertEquals(0, latch2.getCount());
            }
            finally
            {
                inbound.close().get(10, SECONDS);
                outbound.close(false).get(10, SECONDS);
            }
        });
    }

    @Test
    public void testRecoverableCorruptedMessageDelivery() throws Throwable
    {
        test((inbound, outbound, endpoint) -> {
            int version = outbound.settings().acceptVersions.max;
            if (version < VERSION_40)
                return;

            AtomicInteger counter = new AtomicInteger();
            unsafeSetSerializer(Verb._TEST_1, () -> new IVersionedSerializer<Object>()
            {
                public void serialize(Object o, DataOutputPlus out, int version) throws IOException
                {
                    out.writeInt((Integer) o);
                }

                public Object deserialize(DataInputPlus in, int version) throws IOException
                {
                    if (counter.getAndIncrement() == 3)
                        throw new UnknownColumnException("");

                    return in.readInt();
                }

                public long serializedSize(Object o, int version)
                {
                    return Integer.BYTES;
                }
            });

            // Connect
            connect(outbound);

            CountDownLatch latch = new CountDownLatch(4);
            unsafeSetHandler(Verb._TEST_1, () -> message -> latch.countDown());
            for (int i = 0; i < 5; i++)
                outbound.enqueue(Message.out(Verb._TEST_1, 0xffffffff));

            latch.await(10, SECONDS);
            Assert.assertEquals(0, latch.getCount());
            Assert.assertEquals(6, counter.get());
        });
    }

    @Test
    public void testCRCCorruption() throws Throwable
    {
        test((inbound, outbound, endpoint) -> {
            int version = outbound.settings().acceptVersions.max;
            if (version < VERSION_40)
                return;

            unsafeSetSerializer(Verb._TEST_1, () -> new IVersionedSerializer<Object>()
            {
                public void serialize(Object o, DataOutputPlus out, int version) throws IOException
                {
                    out.writeInt((Integer) o);
                }

                public Object deserialize(DataInputPlus in, int version) throws IOException
                {
                    return in.readInt();
                }

                public long serializedSize(Object o, int version)
                {
                    return Integer.BYTES;
                }
            });

            connect(outbound);

            outbound.unsafeGetChannel().pipeline().addFirst(new ChannelOutboundHandlerAdapter() {
                public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
                    ByteBuf bb = (ByteBuf) msg;
                    bb.setByte(0, 0xAB);
                    ctx.write(msg, promise);
                }
            });
            outbound.enqueue(Message.out(Verb._TEST_1, 0xffffffff));
            CompletableFuture.runAsync(() -> {
                while (outbound.isConnected() && !Thread.interrupted()) {}
            }).get(10, SECONDS);
            Assert.assertFalse(outbound.isConnected());
            // TODO: count corruptions

            connect(outbound);
        });
    }

    @Test
    public void testAcquireReleaseOutbound() throws Throwable
    {
        // In each test round, K capacity is reserved upfront.
        // Two groups of threads each release/acquire for K capacity in total accordingly,
        //   i.e. if only the release threads run, at the end, the reserved capacity is 0 (K - K).
        // During the test, we expect N (N <= maxFailures) acquire attempts (for M capacity) to fail.
        // The reserved capacity (pendingBytes) at the end of the round should equal to K - N * M,
        //   which you can find in the assertion.
        test((inbound, outbound, endpoint) -> {
            // max capacity equals to permit-free sendQueueCapcity + the minimun of endpoint and global reserve
            double maxSendQueueCapacity = outbound.settings().applicationSendQueueCapacityInBytes +
                                          Double.min(outbound.settings().applicationSendQueueReserveEndpointCapacityInBytes,
                                                     outbound.settings().applicationSendQueueReserveGlobalCapacityInBytes.limit());
            int concurrency = 100;
            int attempts = 10000;
            int acquireCount = concurrency * attempts;
            long acquireStep = Math.round(maxSendQueueCapacity * 1.2 / acquireCount / 2); // It is guranteed to acquire (~20%) more
            // The total overly acquired amount divides the amount acquired in each step. Get the ceil value so not to miss the acquire that just exceeds.
            long maxFailures = (long) Math.ceil((acquireCount * acquireStep * 2 - maxSendQueueCapacity) / acquireStep); // The result must be in the range of lone
            AtomicLong acquisitionFailures = new AtomicLong();
            Runnable acquirer = () -> {
                for (int j = 0; j < attempts; j++)
                {
                    if (!outbound.unsafeAcquireCapacity(acquireStep))
                        acquisitionFailures.incrementAndGet();
                }
            };
            Runnable releaser = () -> {
                for (int j = 0; j < attempts; j++)
                    outbound.unsafeReleaseCapacity(acquireStep);
            };

            // Start N acquirer and releaser to contend for capcaity
            List<Runnable> submitOrder = new ArrayList<>(concurrency * 2);
            for (int i = 0 ; i < concurrency ; ++i)
                submitOrder.add(acquirer);
            for (int i = 0 ; i < concurrency ; ++i)
                submitOrder.add(releaser);
            // randomize their start order
            randomize(submitOrder);

            try
            {
                // Reserve enough capacity upfront to ensure the releaser threads cannot release all reserved capacity.
                // i.e. the pendingBytes is always positive during the test.
                Assert.assertTrue("Unable to reserve enough capacity",
                                  outbound.unsafeAcquireCapacity(acquireCount, acquireCount * acquireStep));
                ExecutorService executor = Executors.newFixedThreadPool(concurrency);

                submitOrder.forEach(executor::submit);

                executor.shutdown();
                Assert.assertTrue(executor.awaitTermination(1, TimeUnit.MINUTES));

                Assert.assertEquals(acquireCount * acquireStep - (acquisitionFailures.get() * acquireStep), outbound.pendingBytes());
                Assert.assertEquals(acquireCount - acquisitionFailures.get(), outbound.pendingCount());
                Assert.assertTrue(String.format("acquisitionFailures should be capped by maxFailure. acquisitionFailures: %d, acquisitionFailures: %d",
                                                maxFailures, acquisitionFailures.get()),
                                  acquisitionFailures.get() <= maxFailures);
            }
            finally
            {   // release the acquired capacity from this round
                outbound.unsafeReleaseCapacity(outbound.pendingCount(), outbound.pendingBytes());
            }
        });
    }

    private static <V> void randomize(List<V> list)
    {
        long seed = ThreadLocalRandom.current().nextLong();
        logger.info("Seed used for randomize: " + seed);
        Random random = new Random(seed);
        switch (random.nextInt(3))
        {
            case 0:
                Collections.shuffle(list, random);
                break;
            case 1:
                Collections.reverse(list);
                break;
            case 2:
                // leave as is
        }
    }

    private void connect(OutboundConnection outbound) throws Throwable
    {
        CountDownLatch latch = new CountDownLatch(1);
        unsafeSetHandler(Verb._TEST_1, () -> message -> latch.countDown());
        outbound.enqueue(Message.out(Verb._TEST_1, 0xffffffff));
        latch.await(10, SECONDS);
        Assert.assertEquals(0, latch.getCount());
        Assert.assertTrue(outbound.isConnected());
    }

}
