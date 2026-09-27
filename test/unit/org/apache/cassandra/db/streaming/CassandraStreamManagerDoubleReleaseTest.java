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
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.streaming.OutgoingStream;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.StreamingChannel;
import org.apache.cassandra.streaming.async.NettyStreamingConnectionFactory;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.Ref;

import static org.apache.cassandra.service.ActiveRepairService.NO_PENDING_REPAIR;
import static org.junit.Assert.fail;

/**
 * Tests that an empty-sections sstable reference is not double-released if a subsequent stream
 * construction failure occurs in {@link CassandraStreamManager#createOutgoingStreams}.
 *
 * <p>When an sstable has no sections for the requested ranges, its reference must be released and
 * removed from {@code refs}. If it is only released without removal, a failure when creating a later
 * stream causes the catch block's {@code refs.release()} to release it a second time, throwing an
 * {@link IllegalStateException}.
 */
@RunWith(BMUnitRunner.class)
public class CassandraStreamManagerDoubleReleaseTest
{
    private static final String table = "tbl";
    private static final StreamingChannel.Factory connectionFactory = new NettyStreamingConnectionFactory();

    private static final int N = 1;
    private static final int RETRY_BUDGET = 20;

    private static final String DOUBLE_RELEASE_MSG = "Attempted to release a reference that has already been released";
    private static final String INJECTED_MSG = "injected component manifest failure";

    // Set once getPositionsForRanges returns empty, ensuring failure is injected only after an empty-sections ref was released.
    private static final AtomicBoolean emptySeen = new AtomicBoolean(false);

    // Byteman callbacks
    public static void markEmptySeen()  { emptySeen.set(true); }
    public static boolean shouldThrow() { return emptySeen.get(); }

    private String keyspace;
    private TableMetadata tbm;
    private ColumnFamilyStore cfs;

    @BeforeClass
    public static void setupClass() throws Exception
    {
        SchemaLoader.prepareServer();
    }

    @Before
    public void createKeyspace()
    {
        keyspace = String.format("ks_%s", System.currentTimeMillis());
        tbm = CreateTableStatement.parse(String.format("CREATE TABLE %s (k INT PRIMARY KEY, v INT)", table), keyspace)
                                  .compaction(CompactionParams.stcs(Collections.emptyMap()))
                                  .build();
        SchemaLoader.createKeyspace(keyspace, KeyspaceParams.simple(1), tbm);
        cfs = Schema.instance.getColumnFamilyStoreInstance(tbm.id);
    }

    @Test
    @BMRules(rules = {
        @BMRule(name = "record empty getPositionsForRanges result",
                targetClass = "org.apache.cassandra.io.sstable.format.SSTableReader",
                targetMethod = "getPositionsForRanges",
                targetLocation = "AT EXIT",
                condition = "$!.isEmpty()",
                action = "org.apache.cassandra.db.streaming.CassandraStreamManagerDoubleReleaseTest.markEmptySeen()"),
        @BMRule(name = "throw from ComponentManifest.create once an empty-sections ref was released",
                targetClass = "org.apache.cassandra.db.streaming.ComponentManifest",
                targetMethod = "create",
                targetLocation = "AT ENTRY",
                condition = "org.apache.cassandra.db.streaming.CassandraStreamManagerDoubleReleaseTest.shouldThrow()",
                action = "throw new java.lang.RuntimeException(\"injected component manifest failure\")")
    })
    public void emptySectionsRefIsNotDoubleReleasedOnExceptionPath() throws Exception
    {
        // Refs iteration order is determined by HashMap bucket layout. Alternating sstable creation order
        // ensures an empty-sections sstable is processed before a failing sstable within two attempts.
        for (int attempt = 0; attempt < RETRY_BUDGET; attempt++)
        {
            RangesAtEndpoint replicas = freshSSTables(attempt);
            emptySeen.set(false);

            Collection<OutgoingStream> streams = null;
            try
            {
                streams = cfs.getStreamManager().createOutgoingStreams(session(NO_PENDING_REPAIR),
                                                                       replicas,
                                                                       NO_PENDING_REPAIR,
                                                                       PreviewKind.NONE);
                // The failing sstable was processed before the empty one, so no exception was injected; retry.
                releaseStreams(streams);
            }
            catch (IllegalStateException e)
            {
                if (e.getMessage() != null && e.getMessage().contains(DOUBLE_RELEASE_MSG))
                    fail("Empty-sections Ref was double-released on exception path: " + e);
                throw e;
            }
            catch (RuntimeException e)
            {
                // Injected failure propagated cleanly without double-releasing any refs.
                if (e.getMessage() != null && e.getMessage().contains(INJECTED_MSG))
                    return;
                throw e;
            }
        }

        fail("Could not process an empty-sections sstable before a failing sstable within " + RETRY_BUDGET + " attempts");
    }

    /**
     * Rebuilds the CFS with N repaired (empty-sections) sstables and N unrepaired (non-empty)
     * sstables, and returns a transient-only replica set. A repaired sstable is streamed against
     * {@code replicas.onlyFull()} (empty here) so its sections are empty; an unrepaired sstable is
     * streamed against {@code replicas.ranges()} (the whole ring) so its sections are non-empty.
     */
    private RangesAtEndpoint freshSSTables(int attempt) throws Exception
    {
        cfs.truncateBlocking();

        long repairedAt = System.currentTimeMillis();
        Runnable makeRepaired = () -> {
            for (int i = 0; i < N; i++)
            {
                final int k = 100 + i;
                SSTableReader repaired = createSSTable(() -> QueryProcessor.executeInternal(
                    String.format("INSERT INTO %s.%s (k, v) VALUES (%d, %d)", keyspace, table, k, k)));
                try
                {
                    mutateRepaired(repaired, repairedAt);
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            }
        };
        Runnable makeUnrepaired = () -> {
            for (int i = 0; i < N; i++)
            {
                final int k = 200 + i;
                createSSTable(() -> QueryProcessor.executeInternal(
                    String.format("INSERT INTO %s.%s (k, v) VALUES (%d, %d)", keyspace, table, k, k)));
            }
        };

        if (attempt % 2 == 0)
        {
            makeRepaired.run();
            makeUnrepaired.run();
        }
        else
        {
            makeUnrepaired.run();
            makeRepaired.run();
        }

        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        Range<Token> wholeRing = new Range<>(partitioner.getMinimumToken(), partitioner.getMinimumToken());
        InetAddressAndPort local = InetAddressAndPort.getByName("127.0.0.1");
        // transient replica (full=false): onlyFull() is empty => repaired sstables get empty sections
        return RangesAtEndpoint.of(new Replica(local, wholeRing, false));
    }

    private SSTableReader createSSTable(Runnable queryable)
    {
        Set<SSTableReader> before = cfs.getLiveSSTables();
        queryable.run();
        Util.flush(cfs);
        Set<SSTableReader> after = cfs.getLiveSSTables();
        return Iterables.getOnlyElement(Sets.difference(after, before));
    }

    private static void mutateRepaired(SSTableReader sstable, long repairedAt) throws IOException
    {
        Descriptor descriptor = sstable.descriptor;
        descriptor.getMetadataSerializer().mutateRepairMetadata(descriptor, repairedAt, NO_PENDING_REPAIR, false);
        sstable.reloadSSTableMetadata();
    }

    private static void releaseStreams(Collection<OutgoingStream> streams)
    {
        if (streams == null)
            return;
        for (OutgoingStream stream : streams)
        {
            Ref<SSTableReader> ref = CassandraOutgoingFile.fromStream(stream).getRef();
            ref.release();
        }
    }

    private static StreamSession session(TimeUUID pendingRepair)
    {
        try
        {
            return new StreamSession(StreamOperation.REPAIR,
                                     InetAddressAndPort.getByName("127.0.0.1"),
                                     connectionFactory,
                                     null,
                                     MessagingService.current_version,
                                     false,
                                     0,
                                     pendingRepair,
                                     PreviewKind.NONE);
        }
        catch (UnknownHostException e)
        {
            throw new AssertionError(e);
        }
    }
}
