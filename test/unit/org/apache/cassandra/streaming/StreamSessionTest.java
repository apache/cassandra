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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.UUIDGen;

public class StreamSessionTest
{
    private String keyspace = null;
    private static final String table = "tbl";

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

    private SSTableReader createSSTable(Runnable queryable)
    {
        Set<SSTableReader> before = cfs.getLiveSSTables();
        queryable.run();
        cfs.forceBlockingFlush();
        Set<SSTableReader> after = cfs.getLiveSSTables();

        Set<SSTableReader> diff = Sets.difference(after, before);
        assert diff.size() == 1 : "Expected 1 new sstable, got " + diff.size();
        return diff.iterator().next();
    }

    private static void mutateRepaired(SSTableReader sstable, long repairedAt, UUID pendingRepair) throws IOException
    {
        Descriptor descriptor = sstable.descriptor;
        descriptor.getMetadataSerializer().mutateRepaired(descriptor, repairedAt, pendingRepair);
        sstable.reloadSSTableMetadata();

    }

    private Set<SSTableReader> selectReaders(UUID pendingRepair)
    {
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        Collection<Range<Token>> ranges = Lists.newArrayList(new Range<Token>(partitioner.getMinimumToken(), partitioner.getMinimumToken()));
        List<StreamSession.SSTableStreamingSections> sections = StreamSession.getSSTableSectionsForRanges(ranges,
                                                                                                          Lists.newArrayList(cfs),
                                                                                                          pendingRepair,
                                                                                                          PreviewKind.NONE);
        Set<SSTableReader> sstables = new HashSet<>();
        for (StreamSession.SSTableStreamingSections section: sections)
        {
            sstables.add(section.ref.get());
        }
        return sstables;
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
        mutateRepaired(sstable4, repairedAt, ActiveRepairService.NO_PENDING_REPAIR);

        // no pending repair should return all sstables
        Assert.assertEquals(Sets.newHashSet(sstable1, sstable2, sstable3, sstable4), selectReaders(ActiveRepairService.NO_PENDING_REPAIR));

        // a pending repair arg should only return sstables with the same pending repair id
        Assert.assertEquals(Sets.newHashSet(sstable2), selectReaders(pendingRepair));
    }
}
