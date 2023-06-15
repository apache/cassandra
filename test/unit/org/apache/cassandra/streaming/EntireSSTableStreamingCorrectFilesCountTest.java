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

package org.apache.cassandra.streaming;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import javax.annotation.Nullable;

import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.streaming.CassandraOutgoingFile;
import org.apache.cassandra.db.streaming.ComponentManifest;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.net.AsyncStreamingOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.SharedDefaultFileRegion;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.streaming.async.NettyStreamingConnectionFactory;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.service.ActiveRepairService.NO_PENDING_REPAIR;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class EntireSSTableStreamingCorrectFilesCountTest
{
    public static final String KEYSPACE = "EntireSSTableStreamingCorrectFilesCountTest";
    public static final String CF_STANDARD = "Standard1";

    private static SSTableReader sstable;
    private static ColumnFamilyStore store;
    private static RangesAtEndpoint rangesAtEndpoint;

    @BeforeClass
    public static void defineSchemaAndPrepareSSTable()
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, CF_STANDARD)
                                                // LeveledCompactionStrategy is important here,
                                                // streaming of entire SSTables works currently only with this strategy
                                                .compaction(CompactionParams.lcs(Collections.emptyMap()))
                                                .partitioner(ByteOrderedPartitioner.instance));

        Keyspace keyspace = Keyspace.open(KEYSPACE);
        store = keyspace.getColumnFamilyStore(CF_STANDARD);

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

        Token start = ByteOrderedPartitioner.instance.getTokenFactory().fromString(Long.toHexString(0));
        Token end = ByteOrderedPartitioner.instance.getTokenFactory().fromString(Long.toHexString(100));

        rangesAtEndpoint = RangesAtEndpoint.toDummyList(Collections.singleton(new Range<>(start, end)));
    }

    @Test
    public void test() throws Exception
    {
        FileCountingStreamEventHandler streamEventHandler = new FileCountingStreamEventHandler();
        StreamSession session = setupStreamingSessionForTest(streamEventHandler);
        Collection<OutgoingStream> outgoingStreams = store.getStreamManager().createOutgoingStreams(session,
                                                                                                    rangesAtEndpoint,
                                                                                                    NO_PENDING_REPAIR,
                                                                                                    PreviewKind.NONE);

        session.addTransferStreams(outgoingStreams);
        AsyncStreamingOutputPlus out = constructDataOutputStream();

        for (OutgoingStream outgoingStream : outgoingStreams)
        {
            outgoingStream.write(session, out, MessagingService.VERSION_40);
            // verify hardlinks are removed after streaming
            Descriptor descriptor = ((CassandraOutgoingFile) outgoingStream).getRef().get().descriptor;
            assertTrue(descriptor.getTemporaryFiles().isEmpty());
        }

        int totalNumberOfFiles = session.transfers.get(store.metadata.id).getTotalNumberOfFiles();

        assertEquals(ComponentManifest.create(sstable).components().size(), totalNumberOfFiles);
        assertEquals(streamEventHandler.fileNames.size(), totalNumberOfFiles);
    }

    private AsyncStreamingOutputPlus constructDataOutputStream()
    {
        // This is needed as Netty releases the ByteBuffers as soon as the channel is flushed
        ByteBuf serializedFile = Unpooled.buffer(8192);
        EmbeddedChannel channel = createMockNettyChannel(serializedFile);
        return new AsyncStreamingOutputPlus(channel)
        {
            public void flush() throws IOException
            {
                // NO-OP
            }
        };
    }

    private EmbeddedChannel createMockNettyChannel(ByteBuf serializedFile)
    {
        WritableByteChannel wbc = new WritableByteChannel()
        {
            private boolean isOpen = true;

            public int write(ByteBuffer src)
            {
                int size = src.limit();
                serializedFile.writeBytes(src);
                return size;
            }

            public boolean isOpen()
            {
                return isOpen;
            }

            public void close()
            {
                isOpen = false;
            }
        };

        return new EmbeddedChannel(new ChannelOutboundHandlerAdapter()
        {
            @Override
            public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception
            {
                ((SharedDefaultFileRegion) msg).transferTo(wbc, 0);
                super.write(ctx, msg, promise);
            }
        });
    }


    private StreamSession setupStreamingSessionForTest(StreamEventHandler streamEventHandler)
    {
        StreamCoordinator streamCoordinator = new StreamCoordinator(StreamOperation.BOOTSTRAP,
                                                                    1,
                                                                    new NettyStreamingConnectionFactory(),
                                                                    false,
                                                                    false,
                                                                    null,
                                                                    PreviewKind.NONE);

        StreamResultFuture future = StreamResultFuture.createInitiator(nextTimeUUID(),
                                                                       StreamOperation.BOOTSTRAP,
                                                                       Collections.singleton(streamEventHandler),
                                                                       streamCoordinator);

        InetAddressAndPort peer = FBUtilities.getBroadcastAddressAndPort();
        streamCoordinator.addSessionInfo(new SessionInfo(peer,
                                                         0,
                                                         peer,
                                                         Collections.emptyList(),
                                                         Collections.emptyList(),
                                                         StreamSession.State.INITIALIZED,
                                                         null));

        StreamSession session = streamCoordinator.getOrCreateOutboundSession(peer);
        session.init(future);

        return session;
    }

    private static final class FileCountingStreamEventHandler implements StreamEventHandler
    {
        final Collection<String> fileNames = new ArrayList<>();

        public void handleStreamEvent(StreamEvent event)
        {
            if (event.eventType == StreamEvent.Type.FILE_PROGRESS && event instanceof StreamEvent.ProgressEvent)
            {
                StreamEvent.ProgressEvent progressEvent = ((StreamEvent.ProgressEvent) event);
                fileNames.add(progressEvent.progress.fileName);
            }
        }

        public void onSuccess(@Nullable StreamState streamState)
        {
            assert streamState != null;
            assertFalse(streamState.hasFailedSession());
        }

        public void onFailure(Throwable throwable)
        {
            fail();
        }
    }
}
