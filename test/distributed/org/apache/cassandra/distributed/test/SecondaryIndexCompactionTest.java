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

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.index.internal.CassandraIndex;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

public class SecondaryIndexCompactionTest extends TestBaseImpl
{
    @Test
    public void test2iCompaction() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(1).start()))
        {
            cluster.schemaChange(withKeyspace("create table %s.tbl (id int, ck int, something int, else int, primary key (id, ck));"));
            cluster.schemaChange(withKeyspace("create index tbl_idx on %s.tbl (ck)"));

            for (int i = 0; i < 10; i++)
                cluster.coordinator(1).execute(withKeyspace("insert into %s.tbl (id, ck, something, else) values (?, ?, ?, ?)"), ConsistencyLevel.ALL, i, i, i, i);

            cluster.get(1).runOnInstance(() -> {
                ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl");
                CassandraIndex i = (CassandraIndex) cfs.indexManager.getIndexByName("tbl_idx");
                i.getIndexCfs().forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
                Set<SSTableReader> idxSSTables = i.getIndexCfs().getLiveSSTables();
                // emulate ongoing index compaction:
                CompactionInfo.Holder h = new MockHolder(i.getIndexCfs().metadata(), idxSSTables);
                CompactionManager.instance.active.beginCompaction(h);
                CompactionManager.instance.active.estimatedRemainingWriteBytes();
                CompactionManager.instance.active.finishCompaction(h);
            });
        }
    }

    static class MockHolder extends CompactionInfo.Holder
    {
        private final Set<SSTableReader> sstables;
        private final TableMetadata metadata;

        public MockHolder(TableMetadata metadata, Set<SSTableReader> sstables)
        {
            this.metadata = metadata;
            this.sstables = sstables;
        }
        @Override
        public CompactionInfo getCompactionInfo()
        {
            return new CompactionInfo(metadata, OperationType.COMPACTION, 0, 1000, nextTimeUUID(), sstables);
        }

        @Override
        public boolean isGlobal()
        {
            return false;
        }
    }
}