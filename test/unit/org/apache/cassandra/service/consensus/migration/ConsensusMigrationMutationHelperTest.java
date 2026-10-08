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

package org.apache.cassandra.service.consensus.migration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import accord.primitives.Txn;
import accord.utils.SimpleBitSets;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.AccordTimestamps;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.serializers.TableMetadatas;
import org.apache.cassandra.service.accord.txn.TxnUpdate;
import org.apache.cassandra.service.accord.txn.TxnWrite;
import org.apache.cassandra.tcm.ClusterMetadata;

import static org.apache.cassandra.db.SerializationHeader.StableHeaderSerializer.STABLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Authored by LLM
 * <p>
 * Plain mutations executed by Accord that do not preserve their timestamps should be normalised once by the coordinator,
 * so that replicas may bind them to the transaction's execution time as they are deserialized, rather than each rebasing
 * them (see {@link TxnWrite.TimeOffsetHeaderDeserializer}); those that preserve their timestamps must be submitted unchanged.
 */
public class ConsensusMigrationMutationHelperTest extends CQLTester
{
    private static final long ORIGINAL_TIMESTAMP = 12345;
    private static final long ORIGINAL_NOW_IN_SEC = 1000;
    private static final int TTL = 100;

    private static final long EXECUTE_AT_TIMESTAMP = 1794920000123456L; // 2026-11-17
    private static final long EXECUTE_AT_NOW_IN_SEC = EXECUTE_AT_TIMESTAMP / 1_000_000L;

    @Test
    public void testOverwriteIsNormalised()
    {
        Mutation mutation = mutation();
        PartitionUpdate original = mutation.getPartitionUpdates().iterator().next();
        TxnUpdate update = toTxnUpdate(mutation, AccordTimestamps.OVERWRITE);
        assertThat(update.hasNormalisedTimes()).isTrue();
        assertThat(update.preserveTimestamps()).isFalse();
        // plain mutations perform no read, so may be executed by replicas
        assertThat(update.permitsReplicaExecution()).isTrue();

        // binding by offset produces the same result as rebasing the original mutation (the path taken before normalisation)
        PartitionUpdate offset = completeWrite(update, original, new TxnWrite.TimeOffsetHeaderDeserializer(EXECUTE_AT_TIMESTAMP, EXECUTE_AT_NOW_IN_SEC));
        PartitionUpdate rebased = new PartitionUpdate.Builder(original, 0).updateTimesForAccord(EXECUTE_AT_TIMESTAMP, EXECUTE_AT_NOW_IN_SEC).build();
        assertEquivalent(rebased, offset);

        // and both are bound to the execution time
        int checked = 0;
        for (Row row : offset)
        {
            LivenessInfo liveness = row.primaryKeyLivenessInfo();
            if (!liveness.isEmpty())
            {
                assertThat(liveness.timestamp()).isEqualTo(EXECUTE_AT_TIMESTAMP);
                assertThat(liveness.localExpirationTime()).isEqualTo(EXECUTE_AT_NOW_IN_SEC + TTL);
                ++checked;
            }
            for (Cell<?> cell : row.cells())
            {
                if (cell.isTombstone())
                {
                    assertThat(cell.timestamp()).isEqualTo(EXECUTE_AT_TIMESTAMP - 1);
                    assertThat(cell.localDeletionTime()).isEqualTo(EXECUTE_AT_NOW_IN_SEC);
                }
                else
                {
                    assertThat(cell.timestamp()).isEqualTo(EXECUTE_AT_TIMESTAMP);
                    assertThat(cell.localDeletionTime()).isEqualTo(EXECUTE_AT_NOW_IN_SEC + TTL);
                }
                ++checked;
            }
        }
        assertThat(checked).isEqualTo(3);
    }

    @Test
    public void testPreservedTimestampsAreUnchanged()
    {
        for (AccordTimestamps timestamps : new AccordTimestamps[] { AccordTimestamps.DECLARED, AccordTimestamps.MIXED })
        {
            Mutation mutation = mutation();
            PartitionUpdate original = mutation.getPartitionUpdates().iterator().next();
            TxnUpdate update = toTxnUpdate(mutation, timestamps);
            assertThat(update.hasNormalisedTimes()).isFalse();
            assertThat(update.preserveTimestamps()).isTrue();
            assertEquivalent(original, completeWrite(update, original, STABLE));
        }
    }

    @Test
    public void testNormalisedIsRejected()
    {
        assertThatThrownBy(() -> toTxnUpdate(mutation(), AccordTimestamps.NORMALISED)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testCommitConsistencyLevelPreventsReplicaExecution()
    {
        Mutation mutation = mutation(); // creates the table, so must precede ClusterMetadata.current()
        Txn txn = ConsensusMigrationMutationHelper.toAccordTxn(ClusterMetadata.current(), Collections.singletonList(mutation), ConsistencyLevel.QUORUM, AccordTimestamps.OVERWRITE);
        TxnUpdate update = (TxnUpdate) txn.update();
        assertThat(update.hasNormalisedTimes()).isTrue();
        assertThat(update.permitsReplicaExecution()).isFalse();
        assertThat(update.cassandraCommitCL()).isEqualTo(ConsistencyLevel.QUORUM);
    }

    /**
     * A row with expiring liveness and an expiring cell, and a row with a cell tombstone, at arbitrary times
     */
    private Mutation mutation()
    {
        if (currentTable() == null)
            createTable("CREATE TABLE %s (k int, c int, v int, w int, PRIMARY KEY (k, c))");
        TableMetadata metadata = currentTableMetadata();
        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(metadata, 0)
                                                               .timestamp(ORIGINAL_TIMESTAMP)
                                                               .nowInSec(ORIGINAL_NOW_IN_SEC)
                                                               .ttl(TTL);
        builder.row(0).add("v", 1);
        builder.row(1).noPrimaryKeyLivenessInfo().delete("w");
        return builder.buildAsMutation();
    }

    private static TxnUpdate toTxnUpdate(Mutation mutation, AccordTimestamps timestamps)
    {
        Txn txn = ConsensusMigrationMutationHelper.toAccordTxn(ClusterMetadata.current(), Collections.singletonList(mutation), null, timestamps);
        return (TxnUpdate) txn.update();
    }

    private static PartitionUpdate completeWrite(TxnUpdate update, PartitionUpdate original, SerializationHeader.AbstractStableHeaderSerializer headerSerializer)
    {
        List<PartitionUpdate> writes = new ArrayList<>();
        update.completeWritesForKey(writes, SimpleBitSets.allSet(1), PartitionKey.of(original), TableMetadatas.of(original.metadata()), headerSerializer);
        assertThat(writes).hasSize(1);
        return writes.get(0);
    }

    private static void assertEquivalent(PartitionUpdate expected, PartitionUpdate actual)
    {
        assertThat(actual.partitionKey()).isEqualTo(expected.partitionKey());
        assertThat(actual.partitionLevelDeletion()).isEqualTo(expected.partitionLevelDeletion());
        assertThat(actual.staticRow()).isEqualTo(expected.staticRow());
        List<Unfiltered> expectedUnfiltereds = new ArrayList<>(), actualUnfiltereds = new ArrayList<>();
        try (UnfilteredRowIterator e = expected.unfilteredIterator(); UnfilteredRowIterator a = actual.unfilteredIterator())
        {
            e.forEachRemaining(expectedUnfiltereds::add);
            a.forEachRemaining(actualUnfiltereds::add);
        }
        assertThat(actualUnfiltereds).isEqualTo(expectedUnfiltereds);
    }
}
