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
package org.apache.cassandra.db.streaming;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;
import java.util.Collections;
import java.util.Queue;

import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultFileRegion;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.AsyncStreamingOutputPlus;
import org.apache.cassandra.net.SharedDefaultFileRegion;
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

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CassandraEntireSSTableStreamWriterTest
{
    public static final String KEYSPACE = "CassandraEntireSSTableStreamWriterTest";
    public static final String CF_STANDARD = "Standard1";
    public static final String CF_INDEXED = "Indexed1";
    public static final String CF_STANDARDLOWINDEXINTERVAL = "StandardLowIndexInterval";

    private static SSTableReader sstable;
    private static Descriptor descriptor;
    private static ColumnFamilyStore store;

    @BeforeClass
    public static void defineSchemaAndPrepareSSTable()
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

        Keyspace keyspace = Keyspace.open(KEYSPACE);
        store = keyspace.getColumnFamilyStore("Standard1");

        // insert data and compact to a single sstable
        CompactionManager.instance.disableAutoCompaction();
        for (int j = 0; j < 10; j++)
        {
            new RowUpdateBuilder(store.metadata(), j, String.valueOf(j))
            .clustering("0")
            .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
            .build()
            .applyUnsafe();
        }
        Util.flush(store);
        CompactionManager.instance.performMaximal(store, false);

        sstable = store.getLiveSSTables().iterator().next();
        descriptor = sstable.descriptor;
    }

    @Test
    public void testBlockWriterOverWire() throws IOException
    {
        StreamSession session = setupStreamingSessionForTest();

        EmbeddedChannel channel = new EmbeddedChannel();
        try (AsyncStreamingOutputPlus out = new AsyncStreamingOutputPlus(channel);
             ComponentContext context = ComponentContext.create(sstable))
        {
            CassandraEntireSSTableStreamWriter writer = new CassandraEntireSSTableStreamWriter(sstable, session, context);

            writer.write(out);

            Queue msgs = channel.outboundMessages();

            assertTrue(msgs.peek() instanceof DefaultFileRegion);
        }
    }

    @Test
    public void testBlockReadingAndWritingOverWire() throws Throwable
    {
        StreamSession session = setupStreamingSessionForTest();
        InetAddressAndPort peer = FBUtilities.getBroadcastAddressAndPort();


        // This is needed as Netty releases the ByteBuffers as soon as the channel is flushed
        ByteBuf serializedFile = Unpooled.buffer(8192);
        EmbeddedChannel channel = createMockNettyChannel(serializedFile);
        try (AsyncStreamingOutputPlus out = new AsyncStreamingOutputPlus(channel);
             ComponentContext context = ComponentContext.create(sstable))
        {
            CassandraEntireSSTableStreamWriter writer = new CassandraEntireSSTableStreamWriter(sstable, session, context);
            writer.write(out);

            session.prepareReceiving(new StreamSummary(sstable.metadata().id, 1, 5104));

            CassandraStreamHeader header =
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

            CassandraEntireSSTableStreamReader reader = new CassandraEntireSSTableStreamReader(new StreamMessageHeader(sstable.metadata().id, peer, session.planId(), false, 0, 0, 0, null), header, session);

            SSTableMultiWriter sstableWriter = reader.read(new DataInputBuffer(serializedFile.nioBuffer(), false));
            Collection<SSTableReader> newSstables = sstableWriter.finished();

            assertEquals(1, newSstables.size());
        }
    }

    private EmbeddedChannel createMockNettyChannel(ByteBuf serializedFile) throws Exception
    {
        WritableByteChannel wbc = new WritableByteChannel()
        {
            private boolean isOpen = true;
            public int write(ByteBuffer src) throws IOException
            {
                int size = src.limit();
                serializedFile.writeBytes(src);
                return size;
            }

            public boolean isOpen()
            {
                return isOpen;
            }

            public void close() throws IOException
            {
                isOpen = false;
            }
        };

        return new EmbeddedChannel(new ChannelOutboundHandlerAdapter() {
                @Override
                public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception
                {
                    ((SharedDefaultFileRegion) msg).transferTo(wbc, 0);
                    super.write(ctx, msg, promise);
                }
            });
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
