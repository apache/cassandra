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

package org.apache.cassandra.db.memtable;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InheritingClass;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.dht.Token;

import static org.apache.cassandra.db.memtable.AbstractShardedMemtable.SHARDS_OPTION;

@RunWith(Parameterized.class)
public abstract class TrieMemtableFlushSetTestBase extends CQLTester
{

    static final int partitions = 1_000;
    static final int rowsPerPartition = 2;

    @Parameterized.Parameter(0)
    public int shardsCount;

    @Parameterized.Parameters(name = "shards={0}")
    public static List<Integer> parameters()
    {
        return ImmutableList.of(1, 2, 3, 16);
    }


    public static void configure()
    {
        prePrepareServer();

        LinkedHashMap<String, InheritingClass> memtableConfig = new LinkedHashMap<>();
        for (int shardCount : parameters())
            memtableConfig.put("trie_" + shardCount,
                               new InheritingClass(
                                   null,
                                   TrieMemtable.class.getName(),
                                   Map.of(SHARDS_OPTION, String.valueOf(shardCount))
                               )
            );
        memtableConfig.put("skiplist", new InheritingClass(null, SkipListMemtable.class.getName(), Map.of()));
        DatabaseDescriptor.getRawConfig().memtable = new Config.MemtableOptions();
        DatabaseDescriptor.getRawConfig().memtable.configurations = memtableConfig;
    }

    @Test
    public void checkPartitionCountAndPartitionKeysSizeReportedByFlushSet()
    {
        String keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        String table = createTable(keyspace, "CREATE TABLE %s ( userid bigint, picid bigint, commentid bigint, PRIMARY KEY(userid, picid))" +
                                             " WITH memtable = 'trie_" + shardsCount + "'");

        String tableToCompare = createTable(keyspace, "CREATE TABLE %s ( userid bigint, picid bigint, commentid bigint, PRIMARY KEY(userid, picid))" +
                                             " WITH memtable = 'skiplist'");
        execute("use " + keyspace + ';');

        String writeStatement = "INSERT INTO " + table + "(userid,picid,commentid) VALUES (?,?,?)";
        String writeStatementToCompare = "INSERT INTO " + tableToCompare + "(userid,picid,commentid) VALUES (?,?,?)";


        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        ColumnFamilyStore cfsToCompare = Keyspace.open(keyspace).getColumnFamilyStore(tableToCompare);

        for (long i = 0; i < partitions; ++i)
        {
            for (long j = 0; j < rowsPerPartition; ++j)
            {
                execute(writeStatement, i, j, i + j);
                execute(writeStatementToCompare, i, j, i + j);
            }
        }

        Memtable memtable = cfs.getCurrentMemtable();
        Memtable memtableToCompare = cfsToCompare.getCurrentMemtable();

        {
            Memtable.FlushablePartitionSet<?> flushSetNull = memtable.getFlushSet(null, null);

            Assert.assertEquals(partitions, flushSetNull.partitionCount());
            Assert.assertEquals(partitions * Long.BYTES, flushSetNull.partitionKeysSize());
        }

        {
            Memtable.FlushablePartitionSet<?> flushSetFromMinToMax = memtable.getFlushSet(getMinPosition(cfs), getMaxPosition(cfs));

            Assert.assertEquals(partitions, flushSetFromMinToMax.partitionCount());
            Assert.assertEquals(partitions * Long.BYTES, flushSetFromMinToMax.partitionKeysSize());
        }

        ShardBoundaries shardBoundaries = cfs.localRangeSplits(shardsCount);
        testShardRanges(shardBoundaries, cfs, memtable, memtableToCompare);

        // to test partial covered shards
        ShardBoundaries nonMatchingShardBoundaries1 = cfs.localRangeSplits(13); // a prime number to not match with shard boundaries
        testShardRanges(nonMatchingShardBoundaries1, cfs, memtable, memtableToCompare);

        ShardBoundaries nonMatchingShardBoundaries2 = cfs.localRangeSplits(32); // to check small ranges within shard boundaries
        testShardRanges(nonMatchingShardBoundaries2, cfs, memtable, memtableToCompare);

        // to test the case when a flush position is a partition key
        {
            ByteBuffer oneOfWrittenPartitionKeys = LongType.instance.decompose(0L);
            DecoratedKey decoratedKey = cfs.getPartitioner().decorateKey(oneOfWrittenPartitionKeys);
            testRange(memtable, memtableToCompare, getMinPosition(cfs), decoratedKey);
            testRange(memtable, memtableToCompare, decoratedKey, getMaxPosition(cfs));
        }

    }

    private static void testShardRanges(ShardBoundaries shardBoundaries, ColumnFamilyStore cfs, Memtable memtable, Memtable memtableToCompare)
    {
        for (int fromShardIndex = 0; fromShardIndex < shardBoundaries.shardCount(); fromShardIndex++)
        {
            for  (int toShardIndex = fromShardIndex; toShardIndex < shardBoundaries.shardCount(); toShardIndex++)
            {
                Token from = shardBoundaries.getShardStartBoundary(fromShardIndex);
                PartitionPosition positionFrom = from != null ? from.maxKeyBound() : getMinPosition(cfs);

                Token to = shardBoundaries.getShardEndBoundary(toShardIndex);
                PartitionPosition positionTo = to != null ? to.maxKeyBound() : getMaxPosition(cfs);

                Memtable.FlushablePartitionSet<?> flushSet = memtable.getFlushSet(positionFrom, positionTo);
                Memtable.FlushablePartitionSet<?> flushSetToCompare = memtableToCompare.getFlushSet(positionFrom, positionTo);

                String rangeInfo = "shard id range = [" + fromShardIndex + ", " + toShardIndex + "]" +
                                   ", token range = [" + positionFrom + ", " + positionTo + ")";

                Assert.assertEquals("Number of partition keys does not match for " + rangeInfo,
                                    flushSetToCompare.partitionCount(), flushSet.partitionCount());
                Assert.assertEquals("Total size of partition keys does not match for " + rangeInfo,
                                    flushSetToCompare.partitionKeysSize(), flushSet.partitionKeysSize());
            }
        }
    }

    private static void testRange(Memtable memtable, Memtable memtableToCompare, PartitionPosition positionFrom, PartitionPosition positionTo)
    {
        Memtable.FlushablePartitionSet<?> flushSet = memtable.getFlushSet(positionFrom, positionTo);
        Memtable.FlushablePartitionSet<?> flushSetToCompare = memtableToCompare.getFlushSet(positionFrom, positionTo);

        String rangeInfo = "token range = [" + positionFrom + ", " + positionTo + ")";

        Assert.assertEquals("Number of partition keys does not match for " + rangeInfo,
                            flushSetToCompare.partitionCount(), flushSet.partitionCount());
        Assert.assertEquals("Total size of partition keys does not match for " + rangeInfo,
                            flushSetToCompare.partitionKeysSize(), flushSet.partitionKeysSize());
    }

    private static PartitionPosition getMinPosition(ColumnFamilyStore cfs)
    {
        return cfs.getPartitioner().getMinimumToken().minKeyBound();
    }

    private static PartitionPosition getMaxPosition(ColumnFamilyStore cfs)
    {
        return cfs.getPartitioner().supportsSplitting() ? cfs.getPartitioner().getMaximumTokenForSplitting().maxKeyBound() : null;
    }

}