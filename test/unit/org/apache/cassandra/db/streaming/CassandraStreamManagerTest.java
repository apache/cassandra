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
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.streaming.DefaultConnectionFactory;
import org.apache.cassandra.streaming.OutgoingStream;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.streaming.StreamConnectionFactory;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.concurrent.Ref;

import static org.apache.cassandra.service.ActiveRepairService.NO_PENDING_REPAIR;

public class CassandraStreamManagerTest
{
    private static final String KEYSPACE = null;
    private String keyspace = null;
    private static final String table = "tbl";
    private static final StreamConnectionFactory connectionFactory = new DefaultConnectionFactory();

    private TableMetadata tbm;
    private ColumnFamilyStore cfs;

    @BeforeClass
    public static void setupClass() throws Exception
    {
        SchemaLoader.prepareServer();
    }

    @Before
    public void createKeyspace() throws Exception
    {
        keyspace = String.format("ks_%s", System.currentTimeMillis());
        tbm = CreateTableStatement.parse(String.format("CREATE TABLE %s (k INT PRIMARY KEY, v INT)", table), keyspace).build();
        SchemaLoader.createKeyspace(keyspace, KeyspaceParams.simple(1), tbm);
        cfs = Schema.instance.getColumnFamilyStoreInstance(tbm.id);
    }

    private static StreamSession session(UUID pendingRepair)
    {
        try
        {
            return new StreamSession(StreamOperation.REPAIR,
                                     InetAddressAndPort.getByName("127.0.0.1"),
                                     InetAddressAndPort.getByName("127.0.0.2"),
                                     connectionFactory,
                                     0,
                                     pendingRepair,
                                     PreviewKind.NONE);
        }
        catch (UnknownHostException e)
        {
            throw new AssertionError(e);
        }
    }

    private SSTableReader createSSTable(Runnable queryable)
    {
        Set<SSTableReader> before = cfs.getLiveSSTables();
        queryable.run();
        cfs.forceBlockingFlush();
        Set<SSTableReader> after = cfs.getLiveSSTables();

        Set<SSTableReader> diff = Sets.difference(after, before);
        return Iterables.getOnlyElement(diff);
    }

    private static void mutateRepaired(SSTableReader sstable, long repairedAt, UUID pendingRepair) throws IOException
    {
        Descriptor descriptor = sstable.descriptor;
        descriptor.getMetadataSerializer().mutateRepaired(descriptor, repairedAt, pendingRepair);
        sstable.reloadSSTableMetadata();

    }

    private static Set<SSTableReader> sstablesFromStreams(Collection<OutgoingStream> streams)
    {
        Set<SSTableReader> sstables = new HashSet<>();
        for (OutgoingStream stream: streams)
        {
            Ref<SSTableReader> ref = CassandraOutgoingFile.fromStream(stream).getRef();
            sstables.add(ref.get());
            ref.release();
        }
        return sstables;
    }

    private Set<SSTableReader> getReadersForRange(Range<Token> range)
    {
        Collection<OutgoingStream> streams = cfs.getStreamManager().createOutgoingStreams(session(NO_PENDING_REPAIR),
                                                                                          Collections.singleton(range),
                                                                                          NO_PENDING_REPAIR,
                                                                                          PreviewKind.NONE);
        return sstablesFromStreams(streams);
    }

    private Set<SSTableReader> selectReaders(UUID pendingRepair)
    {
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        Collection<Range<Token>> ranges = Lists.newArrayList(new Range<Token>(partitioner.getMinimumToken(), partitioner.getMinimumToken()));
        Collection<OutgoingStream> streams = cfs.getStreamManager().createOutgoingStreams(session(pendingRepair), ranges, pendingRepair, PreviewKind.NONE);
        return sstablesFromStreams(streams);
    }

    @Test
    public void incrementalSSTableSelection() throws Exception
    {
        // make 3 tables, 1 unrepaired, 2 pending repair with different repair ids, and 1 repaired
        SSTableReader sstable1 = createSSTable(() -> QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (k, v) VALUES (1, 1)", keyspace, table)));
        SSTableReader sstable2 = createSSTable(() -> QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (k, v) VALUES (2, 2)", keyspace, table)));
        SSTableReader sstable3 = createSSTable(() -> QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (k, v) VALUES (3, 3)", keyspace, table)));
        SSTableReader sstable4 = createSSTable(() -> QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (k, v) VALUES (4, 4)", keyspace, table)));


        UUID pendingRepair = UUIDGen.getTimeUUID();
        long repairedAt = System.currentTimeMillis();
        mutateRepaired(sstable2, ActiveRepairService.UNREPAIRED_SSTABLE, pendingRepair);
        mutateRepaired(sstable3, ActiveRepairService.UNREPAIRED_SSTABLE, UUIDGen.getTimeUUID());
        mutateRepaired(sstable4, repairedAt, NO_PENDING_REPAIR);



        // no pending repair should return all sstables
        Assert.assertEquals(Sets.newHashSet(sstable1, sstable2, sstable3, sstable4), selectReaders(NO_PENDING_REPAIR));

        // a pending repair arg should only return sstables with the same pending repair id
        Assert.assertEquals(Sets.newHashSet(sstable2), selectReaders(pendingRepair));
    }

    @Test
    public void testSSTableSectionsForRanges() throws Exception
    {
        cfs.truncateBlocking();

        createSSTable(() -> {
            QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (k, v) VALUES (1, 1)", keyspace, table));
            QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (k, v) VALUES (2, 2)", keyspace, table));
        });

        Collection<SSTableReader> allSSTables = cfs.getLiveSSTables();
        Assert.assertEquals(1, allSSTables.size());
        final Token firstToken = allSSTables.iterator().next().first.getToken();
        DatabaseDescriptor.setSSTablePreempiveOpenIntervalInMB(1);

        Set<SSTableReader> sstablesBeforeRewrite = getReadersForRange(new Range<>(firstToken, firstToken));
        Assert.assertEquals(1, sstablesBeforeRewrite.size());
        final AtomicInteger checkCount = new AtomicInteger();
        // needed since we get notified when compaction is done as well - we can't get sections for ranges for obsoleted sstables
        final AtomicBoolean done = new AtomicBoolean(false);
        final AtomicBoolean failed = new AtomicBoolean(false);
        Runnable r = new Runnable()
        {
            public void run()
            {
                while (!done.get())
                {
                    Range<Token> range = new Range<Token>(firstToken, firstToken);
                    Set<SSTableReader> sstables = getReadersForRange(range);
                    if (sstables.size() != 1)
                        failed.set(true);
                    checkCount.incrementAndGet();
                    Uninterruptibles.sleepUninterruptibly(5, TimeUnit.MILLISECONDS);
                }
            }
        };
        Thread t = NamedThreadFactory.createThread(r);
        try
        {
            t.start();
            cfs.forceMajorCompaction();
            // reset
        }
        finally
        {
            DatabaseDescriptor.setSSTablePreempiveOpenIntervalInMB(50);
            done.set(true);
            t.join(20);
        }
        Assert.assertFalse(failed.get());
        Assert.assertTrue(checkCount.get() >= 2);
        cfs.truncateBlocking();
    }
}
