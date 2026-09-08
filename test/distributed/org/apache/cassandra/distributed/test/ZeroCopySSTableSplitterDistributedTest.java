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

import java.util.Random;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter;
import org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter.Child;
import org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter.Result;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ZeroCopySSTableSplitterDistributedTest extends TestBaseImpl
{
    @Test
    public void publishedSplitChildrenServeReplicaReads() throws Throwable
    {
        try (ICluster<IInvokableInstance> cluster = init(builder().withNodes(2).start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".split_test " +
                                 "(pk int, ck int, v text, PRIMARY KEY (pk, ck)) " +
                                 "WITH compression = {'class': 'LZ4Compressor', 'chunk_length_in_kb': '4'}");
            cluster.get(1).runOnInstance(() -> Keyspace.open(KEYSPACE)
                                                     .getColumnFamilyStore("split_test")
                                                     .disableAutoCompaction());

            Random random = new Random(0x5EED);
            char[] chars = new char[480];
            for (int i = 0; i < chars.length; i++)
                chars[i] = (char) ('!' + random.nextInt(94));
            String value = new String(chars);
            for (int partition = 0; partition < 120; partition++)
            {
                for (int clustering = 0; clustering < 3; clustering++)
                {
                    cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE +
                                                   ".split_test (pk, ck, v) VALUES (?, ?, ?)",
                                                   ConsistencyLevel.ALL, partition, clustering, value);
                }
            }
            assertEquals(0, cluster.get(1).nodetool("flush", KEYSPACE, "split_test"));
            assertEquals(0, cluster.get(1).nodetool("compact", KEYSPACE, "split_test"));

            int[] splitState = cluster.get(1).callOnInstance(() -> splitAndPublish(KEYSPACE, "split_test"));
            assertTrue("size-based split did not increase the SSTable count", splitState[1] > splitState[0]);
            assertTrue("split produced no retained-prefix child", splitState[2] > 0);

            Object[][] rows = cluster.coordinator(1).execute("SELECT pk, ck, v FROM " + KEYSPACE + ".split_test",
                                                              ConsistencyLevel.ALL);
            assertEquals(120 * 3, rows.length);
        }
    }

    private static int[] splitAndPublish(String keyspace, String table)
    {
        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        Set<SSTableReader> live = cfs.getLiveSSTables();
        int originals = live.size();
        int children = 0;
        int prefixed = 0;
        for (SSTableReader parent : live.toArray(new SSTableReader[0]))
        {
            LifecycleTransaction transaction = cfs.getTracker().tryModify(parent, OperationType.COMPACTION);
            if (transaction == null)
                throw new AssertionError("could not acquire split parent " + parent);

            boolean committed = false;
            try
            {
                long targetSize = Math.max(1, parent.descriptor.fileFor(Components.DATA).length() / 3);
                Result result = ZeroCopySSTableSplitter.splitBySize(parent, targetSize, transaction);
                children += result.children.size();
                for (Child child : result.children)
                {
                    transaction.update(child.reader, false);
                    if (child.reader.hasSplitPrefix())
                        prefixed++;
                }
                transaction.obsoleteOriginals();
                transaction.prepareToCommit();
                transaction.commit();
                committed = true;
            }
            finally
            {
                if (!committed)
                    transaction.abort();
            }
        }
        return new int[]{ originals, children, prefixed };
    }
}
