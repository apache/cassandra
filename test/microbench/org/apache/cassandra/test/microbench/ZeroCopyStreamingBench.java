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

package org.apache.cassandra.test.microbench;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultFileRegion;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.streaming.CassandraEntireSSTableStreamReader;
import org.apache.cassandra.db.streaming.CassandraEntireSSTableStreamWriter;
import org.apache.cassandra.db.streaming.CassandraStreamHeader;
import org.apache.cassandra.db.streaming.CassandraStreamReader;
import org.apache.cassandra.db.streaming.CassandraStreamWriter;
import org.apache.cassandra.db.streaming.ComponentContext;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.AsyncStreamingInputPlus;
import org.apache.cassandra.net.AsyncStreamingOutputPlus;
import org.apache.cassandra.schema.CachingParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.streaming.SessionInfo;
import org.apache.cassandra.streaming.StreamCoordinator;
import org.apache.cassandra.streaming.StreamEventHandler;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.StreamSummary;
import org.apache.cassandra.streaming.async.NettyStreamingConnectionFactory;
import org.apache.cassandra.streaming.messages.StreamMessageHeader;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

/**
 * Please ensure that this benchmark is run with entire_sstable_stream_throughput_outbound
 * set to a really high value otherwise, throttling will kick in and the results will not be meaningful.
 */
@Warmup(iterations = 1, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@Threads(1)
public class ZeroCopyStreamingBench
{
    static final int STREAM_SIZE = 50 * 1024 * 1024;

    @State(Scope.Thread)
    public static class BenchmarkState
    {
        public static final String KEYSPACE = "ZeroCopyStreamingBenchmark";
        public static final String CF_STANDARD = "Standard1";
        public static final String CF_INDEXED = "Indexed1";
        public static final String CF_STANDARDLOWINDEXINTERVAL = "StandardLowIndexInterval";

        private static SSTableReader sstable;
        private static ColumnFamilyStore store;
        private StreamSession session;
        private CassandraEntireSSTableStreamWriter blockStreamWriter;
        private ComponentContext context;
        private ByteBuf serializedBlockStream;
        private InetAddressAndPort peer = FBUtilities.getBroadcastAddressAndPort();
        private CassandraEntireSSTableStreamReader blockStreamReader;
        private CassandraStreamWriter partialStreamWriter;
        private CassandraStreamReader partialStreamReader;
        private ByteBuf serializedPartialStream;

        @Setup
        public void setupBenchmark() throws IOException
        {
            Keyspace keyspace = setupSchemaAndKeySpace();
            store = keyspace.getColumnFamilyStore("Standard1");
            generateData();

            sstable = store.getLiveSSTables().iterator().next();
            session = setupStreamingSessionForTest();
            context = ComponentContext.create(sstable);
            blockStreamWriter = new CassandraEntireSSTableStreamWriter(sstable, session, context);

            CapturingNettyChannel blockStreamCaptureChannel = new CapturingNettyChannel(STREAM_SIZE);
            AsyncStreamingOutputPlus out = new AsyncStreamingOutputPlus(blockStreamCaptureChannel);
            blockStreamWriter.write(out);
            serializedBlockStream = blockStreamCaptureChannel.getSerializedStream();
            out.close();

            session.prepareReceiving(new StreamSummary(sstable.metadata().id, 1, serializedBlockStream.readableBytes()));

            CassandraStreamHeader entireSSTableStreamHeader =
                CassandraStreamHeader.builder()
                                     .withSSTableVersion(sstable.descriptor.version)
                                     .withSSTableLevel(0)
                                     .withEstimatedKeys(sstable.estimatedKeys())
                                     .withSections(Collections.emptyList())
                                     .withSerializationHeader(sstable.header.toComponent())
                                     .withComponentManifest(context.manifest())
                                     .isEntireSSTable(true)
                                     .withFirstKey(sstable.getFirst())
                                     .withTableId(sstable.metadata().id)
                                     .build();

            blockStreamReader = new CassandraEntireSSTableStreamReader(new StreamMessageHeader(sstable.metadata().id,
                                                                                               peer, session.planId(), false,
                                                                                               0, 0, 0,
                                                                                               null), entireSSTableStreamHeader, session);

            List<Range<Token>> requestedRanges = Arrays.asList(new Range<>(sstable.getFirst().minValue().getToken(), sstable.getLast().getToken()));
            CassandraStreamHeader partialSSTableStreamHeader =
            CassandraStreamHeader.builder()
                                 .withSSTableVersion(sstable.descriptor.version)
                                 .withSSTableLevel(0)
                                 .withEstimatedKeys(sstable.estimatedKeys())
                                 .withSections(sstable.getPositionsForRanges(requestedRanges))
                                 .withSerializationHeader(sstable.header.toComponent())
                                 .withTableId(sstable.metadata().id)
                                 .build();

            partialStreamWriter = new CassandraStreamWriter(sstable, partialSSTableStreamHeader, session);

            CapturingNettyChannel partialStreamChannel = new CapturingNettyChannel(STREAM_SIZE);
            partialStreamWriter.write(new AsyncStreamingOutputPlus(partialStreamChannel));
            serializedPartialStream = partialStreamChannel.getSerializedStream();

            partialStreamReader = new CassandraStreamReader(new StreamMessageHeader(sstable.metadata().id,
                                                                                    peer, session.planId(), false,
                                                                                    0, 0, 0,
                                                                                    null),
                                                            partialSSTableStreamHeader, session);
        }

        private Keyspace setupSchemaAndKeySpace()
        {
            SchemaLoader.prepareServer();
            SchemaLoader.createKeyspace(KEYSPACE,
                                        KeyspaceParams.simple(1),
                                        SchemaLoader.standardCFMD(KEYSPACE, CF_STANDARD),
                                        SchemaLoader.compositeIndexCFMD(KEYSPACE, CF_INDEXED, true),
                                        SchemaLoader.standardCFMD(KEYSPACE, CF_STANDARDLOWINDEXINTERVAL)
                                                    .minIndexInterval(8)
                                                    .maxIndexInterval(256)
                                                    .caching(CachingParams.CACHE_NOTHING));

            return Keyspace.open(KEYSPACE);
        }

        private void generateData()
        {
            // insert data and compact to a single sstable
            CompactionManager.instance.disableAutoCompaction();
            for (int j = 0; j < 1_000_000; j++)
            {
                new RowUpdateBuilder(store.metadata(), j, String.valueOf(j))
                .clustering("0")
                .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                .build()
                .applyUnsafe();
            }
            store.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);
            CompactionManager.instance.performMaximal(store, false);
        }

        @TearDown
        public void tearDown() throws IOException
        {
            context.close();
            SchemaLoader.cleanupAndLeaveDirs();
            CommitLog.instance.stopUnsafe(true);
        }

        private StreamSession setupStreamingSessionForTest()
        {
            StreamCoordinator streamCoordinator = new StreamCoordinator(StreamOperation.BOOTSTRAP, 1, new NettyStreamingConnectionFactory(), false, false, null, PreviewKind.NONE);
            StreamResultFuture future = StreamResultFuture.createInitiator(nextTimeUUID(), StreamOperation.BOOTSTRAP, Collections.<StreamEventHandler>emptyList(), streamCoordinator);

            InetAddressAndPort peer = FBUtilities.getBroadcastAddressAndPort();
            streamCoordinator.addSessionInfo(new SessionInfo(peer, 0, peer, Collections.emptyList(), Collections.emptyList(), StreamSession.State.INITIALIZED, null));

            StreamSession session = streamCoordinator.getOrCreateOutboundSession(peer);
            session.init(future);
            return session;
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void blockStreamWriter(BenchmarkState state) throws Exception
    {
        EmbeddedChannel channel = createMockNettyChannel();
        AsyncStreamingOutputPlus out = new AsyncStreamingOutputPlus(channel);
        state.blockStreamWriter.write(out);
        out.close();
        channel.finishAndReleaseAll();
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void blockStreamReader(BenchmarkState state) throws Throwable
    {
        EmbeddedChannel channel = createMockNettyChannel();
        AsyncStreamingInputPlus in = new AsyncStreamingInputPlus(channel);
        in.append(state.serializedBlockStream.retainedDuplicate());
        SSTableMultiWriter sstableWriter = state.blockStreamReader.read(in);
        Collection<SSTableReader> newSstables = sstableWriter.finished();
        in.close();
        channel.finishAndReleaseAll();
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void partialStreamWriter(BenchmarkState state) throws Exception
    {
        EmbeddedChannel channel = createMockNettyChannel();
        AsyncStreamingOutputPlus out = new AsyncStreamingOutputPlus(channel);
        state.partialStreamWriter.write(out);
        out.close();
        channel.finishAndReleaseAll();
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void partialStreamReader(BenchmarkState state) throws Throwable
    {
        EmbeddedChannel channel = createMockNettyChannel();
        AsyncStreamingInputPlus in = new AsyncStreamingInputPlus(channel);
        in.append(state.serializedPartialStream.retainedDuplicate());
        SSTableMultiWriter sstableWriter = state.partialStreamReader.read(in);
        Collection<SSTableReader> newSstables = sstableWriter.finished();
        in.close();
        channel.finishAndReleaseAll();
    }

    private EmbeddedChannel createMockNettyChannel()
    {
        EmbeddedChannel channel = new EmbeddedChannel();
        channel.config().setWriteBufferHighWaterMark(STREAM_SIZE); // avoid blocking
        return channel;
    }

    private static class CapturingNettyChannel extends EmbeddedChannel
    {
        private final ByteBuf serializedStream;
        private final WritableByteChannel proxyWBC = new WritableByteChannel()
        {
            public int write(ByteBuffer src) throws IOException
            {
                int rem = src.remaining();
                serializedStream.writeBytes(src);
                return rem;
            }

            public boolean isOpen()
            {
                return true;
            }

            public void close() throws IOException
            {
            }
        };

        public CapturingNettyChannel(int capacity)
        {
            this.serializedStream = alloc().buffer(capacity);
            this.pipeline().addLast(new ChannelOutboundHandlerAdapter()
            {
                @Override
                public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception
                {
                    if (msg instanceof ByteBuf)
                        serializedStream.writeBytes((ByteBuf) msg);
                    else if (msg instanceof ByteBuffer)
                        serializedStream.writeBytes((ByteBuffer) msg);
                    else if (msg instanceof DefaultFileRegion)
                        ((DefaultFileRegion) msg).transferTo(proxyWBC, 0);
                }
            });
            config().setWriteBufferHighWaterMark(capacity);
        }

        public ByteBuf getSerializedStream()
        {
            return serializedStream.copy();
        }
    }
}
