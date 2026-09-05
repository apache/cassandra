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

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableStreamRebuildState.State;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.streaming.OutgoingStream;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.StreamingChannel;
import org.apache.cassandra.streaming.async.NettyStreamingConnectionFactory;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.service.ActiveRepairService.NO_PENDING_REPAIR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Verifies the CASSANDRA-21520 entire-sstable (zero-copy) streaming reservation lifecycle in
 * {@link CassandraOutgoingFile} and its rollback in {@link CassandraStreamManager#createOutgoingStreams}.
 *
 * <ul>
 *     <li>Constructing an entire-sstable stream reserves the per-sstable ZCS status; {@code finish()} releases it,
 *     and {@link CassandraOutgoingFile#releaseStreamRebuildStatus()} is idempotent.</li>
 *     <li>When planning outgoing streams fails partway through a multi-sstable batch, the manager's catch block
 *     releases the ZCS status already reserved by the streams constructed so far, so a planning failure cannot
 *     leak the status.</li>
 * </ul>
 */
public class CassandraStreamManagerRebuildStatusTest
{
    private static final String KEYSPACE = "CassandraStreamManagerRebuildStatusTest";
    private static final String CF = "Standard1";
    private static final StreamingChannel.Factory connectionFactory = new NettyStreamingConnectionFactory();

    private static ColumnFamilyStore store;
    private boolean previousStreamEntireSSTables;

    @BeforeClass
    public static void defineSchema()
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, CF));
        store = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF);
        CompactionManager.instance.disableAutoCompaction();
    }

    @Before
    public void setUp()
    {
        previousStreamEntireSSTables = DatabaseDescriptor.streamEntireSSTables();
        DatabaseDescriptor.setStreamEntireSSTables(true);
        store.disableAutoCompaction();
        store.truncateBlocking();
    }

    @After
    public void tearDown()
    {
        DatabaseDescriptor.setStreamEntireSSTables(previousStreamEntireSSTables);
        store.truncateBlocking();
    }

    private static StreamSession session()
    {
        try
        {
            return new StreamSession(StreamOperation.BOOTSTRAP,
                                     InetAddressAndPort.getByName("127.0.0.1"),
                                     connectionFactory,
                                     null,
                                     MessagingService.current_version,
                                     false,
                                     0,
                                     null,
                                     PreviewKind.NONE);
        }
        catch (UnknownHostException e)
        {
            throw new AssertionError(e);
        }
    }

    private RangesAtEndpoint fullRange()
    {
        Token min = store.getPartitioner().getMinimumToken();
        return RangesAtEndpoint.toDummyList(Collections.singleton(new Range<>(min, min)));
    }

    private static void insertAndFlush(int from, int to)
    {
        for (int j = from; j < to; j++)
        {
            new RowUpdateBuilder(store.metadata(), j, String.valueOf(j))
            .clustering("0")
            .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
            .build()
            .applyUnsafe();
        }
        Util.flush(store);
    }

    @Test
    public void entireStreamReservesAndFinishReleasesStatus()
    {
        insertAndFlush(0, 10);
        assertEquals(1, store.getLiveSSTables().size());

        Collection<OutgoingStream> streams = store.getStreamManager()
                                                  .createOutgoingStreams(session(), fullRange(), NO_PENDING_REPAIR, PreviewKind.NONE);
        try
        {
            assertEquals(1, streams.size());
            CassandraOutgoingFile cof = CassandraOutgoingFile.fromStream(streams.iterator().next());

            // Entire-sstable streaming must have engaged (multiple files advertised) and reserved the ZCS status.
            assertTrue("expected entire-sstable streaming to engage for a full-range stream", cof.getNumFiles() > 1);
            SSTableReader sstable = cof.getRef().get();
            assertEquals(State.ZCS_STREAMING, sstable.streamRebuildState().state());
            assertEquals(1, sstable.streamRebuildState().zcsStreamCount());

            // finish() releases the ZCS status exactly once and the sstable returns to NORMAL.
            cof.finish();
            assertEquals(State.NORMAL, sstable.streamRebuildState().state());
            assertEquals(0, sstable.streamRebuildState().zcsStreamCount());

            // releaseStreamRebuildStatus() is idempotent - a second call must not over-release.
            cof.releaseStreamRebuildStatus();
            assertEquals(State.NORMAL, sstable.streamRebuildState().state());
            assertEquals(0, sstable.streamRebuildState().zcsStreamCount());
        }
        finally
        {
            releaseAll(streams);
        }
    }

    @Test
    public void planningFailurePartwayReleasesAlreadyReservedStreams()
    {
        // Two sstables so the manager's construction loop runs more than once.
        insertAndFlush(0, 10);
        insertAndFlush(10, 20);
        assertEquals(2, store.getLiveSSTables().size());

        // Spy the session so the FIRST outgoing stream is built normally (reserving its ZCS status), then the
        // SECOND construction fails - getStreamOperation() is only called from inside the construction loop, once
        // per sstable, so this deterministically injects a partway planning failure.
        StreamSession spy = Mockito.spy(session());
        RuntimeException boom = new RuntimeException("injected planning failure");
        Mockito.doReturn(StreamOperation.BOOTSTRAP)
               .doThrow(boom)
               .when(spy).getStreamOperation();

        try
        {
            store.getStreamManager().createOutgoingStreams(spy, fullRange(), NO_PENDING_REPAIR, PreviewKind.NONE);
            fail("Expected the injected planning failure to propagate");
        }
        catch (RuntimeException e)
        {
            assertSame(boom, e);
        }

        // The catch block must have released the ZCS status reserved by the already-constructed first stream, so
        // no live sstable is left stuck in ZCS_STREAMING.
        for (SSTableReader sstable : store.getLiveSSTables())
        {
            assertEquals("planning failure must not leak the ZCS streaming status for " + sstable.descriptor,
                         State.NORMAL, sstable.streamRebuildState().state());
            assertEquals(0, sstable.streamRebuildState().zcsStreamCount());
        }
    }

    @Test
    public void streamingFallsBackToLegacyWhenRebuildInProgress()
    {
        insertAndFlush(0, 10);
        assertEquals(1, store.getLiveSSTables().size());
        SSTableReader sstable = store.getLiveSSTables().iterator().next();

        // Model an in-progress SAI rebuild by reserving the rebuild status. The stream must degrade to legacy and
        // must NOT reserve (or later release) any streaming status.
        assertTrue(sstable.streamRebuildState().tryBeginRebuild());

        Collection<OutgoingStream> streams = store.getStreamManager()
                                                  .createOutgoingStreams(session(), fullRange(), NO_PENDING_REPAIR, PreviewKind.NONE);
        try
        {
            assertEquals(1, streams.size());
            CassandraOutgoingFile cof = CassandraOutgoingFile.fromStream(streams.iterator().next());
            assertEquals("stream must fall back to legacy (single file) while a rebuild holds the sstable",
                         1, cof.getNumFiles());
            // Status is still owned by the rebuild, untouched by the stream.
            assertEquals(State.REBUILDING, sstable.streamRebuildState().state());

            cof.finish(); // must be a no-op for the rebuild status
            assertEquals(State.REBUILDING, sstable.streamRebuildState().state());
        }
        finally
        {
            releaseAll(streams);
            sstable.streamRebuildState().endRebuild();
        }
    }

    private static void releaseAll(Collection<OutgoingStream> streams)
    {
        List<OutgoingStream> list = new ArrayList<>(streams);
        for (OutgoingStream stream : list)
        {
            CassandraOutgoingFile cof = CassandraOutgoingFile.fromStream(stream);
            cof.releaseStreamRebuildStatus();
            try
            {
                cof.getRef().release();
            }
            catch (Throwable ignore)
            {
                // ref may already be released via finish() in the test body
            }
        }
    }
}
