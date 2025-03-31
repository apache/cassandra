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

package org.apache.cassandra.distributed.test.tracking;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.primitives.Ints;

import org.apache.cassandra.replication.*;
import org.junit.Assert;
import org.junit.Assume;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.metrics.ReadRepairMetrics;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.replication.MutationSummary.CoordinatorSummary;
import org.apache.cassandra.schema.ReplicationType;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

public class MutationTrackingUtils
{
    private static final int VERSION = MessagingService.current_version;

    public static byte[] encodeId(MutationId id)
    {
        int size = Ints.checkedCast(MutationId.serializer.serializedSize(id, VERSION));
        ByteBuffer buffer = ByteBuffer.allocate(size);
        try (DataOutputBuffer dob = new DataOutputBuffer(buffer))
        {
            MutationId.serializer.serialize(id, dob, VERSION);
            return buffer.array();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static MutationId decodeId(byte[] bytes)
    {
        try (DataInputBuffer dib = new DataInputBuffer(bytes))
        {
            MutationId id = MutationId.serializer.deserialize(dib, VERSION);
            Assert.assertEquals(MutationId.serializer.serializedSize(id, VERSION), bytes.length);
            return id;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static byte[] encodeSummary(MutationSummary summary)
    {
        int size = Ints.checkedCast(MutationSummary.serializer.serializedSize(summary, VERSION));
        ByteBuffer buffer = ByteBuffer.allocate(size);
        try (DataOutputBuffer dob = new DataOutputBuffer(buffer))
        {
            MutationSummary.serializer.serialize(summary, dob, VERSION);
            Assert.assertEquals(size, buffer.position());
            return buffer.array();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static MutationSummary decodeSummary(byte[] bytes)
    {
        try (DataInputBuffer dib = new DataInputBuffer(bytes))
        {
            MutationSummary summary = MutationSummary.serializer.deserialize(dib, VERSION);
            Assert.assertEquals(MutationSummary.serializer.serializedSize(summary, VERSION), bytes.length);
            return summary;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static MutationSummary summaryForKey(String keyspaceName, String tableName, DecoratedKey dk)
    {
        TableMetadata table = Schema.instance.getTableMetadata(keyspaceName, tableName);
        return MutationTrackingService.instance.createSummaryForKey(dk, table.id, false);
    }

    public static MutationSummary summaryForTable(String keyspaceName, String tableName)
    {
        Range<Token> range = new Range<>(Murmur3Partitioner.instance.getMinimumToken(), Murmur3Partitioner.instance.getMinimumToken());
        return summaryForRange(keyspaceName, tableName, range);
    }

    public static MutationSummary summaryForKey(IInvokableInstance node, String keyspaceName, String tableName, int key)
    {
        byte[] encodedSummary = node.callOnInstance(() -> {
            DecoratedKey dk = Murmur3Partitioner.instance.decorateKey(ByteBufferUtil.bytes(key));
            MutationSummary summary = summaryForKey(keyspaceName, tableName, dk);
            return encodeSummary(summary);
        });

        return decodeSummary(encodedSummary);
    }

    public static MutationSummary summaryForTable(IInvokableInstance node, String keyspaceName, String tableName)
    {
        byte[] encodedSummary = node.callOnInstance(() -> {
            MutationSummary summary = summaryForTable(keyspaceName, tableName);
            return encodeSummary(summary);
        });
        return decodeSummary(encodedSummary);
    }

    public static MutationSummary summaryForKey(String keyspaceName, String tableName, int key)
    {
        return summaryForKey(keyspaceName, tableName, Murmur3Partitioner.instance.decorateKey(ByteBufferUtil.bytes(key)));
    }

    public static MutationSummary summaryForRange(String keyspaceName, String tableName, Range<Token> range)
    {
        TableMetadata table = Schema.instance.getTableMetadata(keyspaceName, tableName);
        return MutationTrackingService.instance.createSummaryForRange(range, table.id, false);
    }

    public static Offsets summaryIdSpace(CoordinatorSummary summary)
    {
        return Offsets.union(summary.reconciled, summary.unreconciled);
    }

    public static Map<CoordinatorLogId, Offsets> summaryIdSpace(MutationSummary summary)
    {
        Map<CoordinatorLogId, Offsets> idSpace = new HashMap<>();
        for (int i=0; i<summary.size(); i++)
        {
            CoordinatorSummary coordinatorSummary = summary.get(i);
            idSpace.put(coordinatorSummary.logId(), summaryIdSpace(coordinatorSummary));
        }

        return idSpace;
    }

    public static void assertSummaryContents(MutationSummary summary, Collection<MutationId> expected)
    {
        Assert.assertEquals(expected.size(), summary.unreconciledIds());
        for (MutationId id : expected)
        {
            if (!summary.contains(id))
                throw new AssertionError(String.format("%s doesn't contain %s", summary, id));
        }
    }

    public static void assertIdsForKey(IInvokableInstance node, String keyspaceName, String tableName, int key, Set<MutationId> expected)
    {
        MutationSummary summary = summaryForKey(node, keyspaceName, tableName, key);
        assertSummaryContents(summary, expected);
    }

    public static void assertMatchingSummaryForKey(IInvokableInstance node, String keyspaceName, String tableName, int key, MutationSummary expected)
    {
        byte[] encodedExpected = encodeSummary(expected);
        node.runOnInstance(() -> {
            MutationSummary decodedExpected = decodeSummary(encodedExpected);
            MutationSummary actual = summaryForKey(keyspaceName, tableName, key);
            Assert.assertEquals(decodedExpected, actual);
        });
    }

    /**
     * Checks that nodes have seen the same ids, regardless of whether they agree on their reconciliation status
     */
    public static void assertMatchingSummaryIdSpaceForKey(IInvokableInstance node, String keyspaceName, String tableName, int key, MutationSummary expected)
    {
        byte[] encodedExpected = encodeSummary(expected);
        node.runOnInstance(() -> {
            MutationSummary decodedExpected = decodeSummary(encodedExpected);
            MutationSummary actual = summaryForKey(keyspaceName, tableName, key);
            Assert.assertEquals(summaryIdSpace(decodedExpected), summaryIdSpace(actual));
        });
    }

    public static void assertMatchingSummaryForTable(IInvokableInstance node, String keyspaceName, String tableName, MutationSummary expected)
    {
        byte[] encodedExpected = encodeSummary(expected);
        node.runOnInstance(() -> {
            MutationSummary decodedExpected = decodeSummary(encodedExpected);
            MutationSummary actual = summaryForTable(keyspaceName, tableName);
            Assert.assertEquals(decodedExpected, actual);
        });
    }

    public static void assertMatchingSummaryIdSpaceForTable(IInvokableInstance node, String keyspaceName, String tableName, MutationSummary expected)
    {
        byte[] encodedExpected = encodeSummary(expected);
        node.runOnInstance(() -> {
            MutationSummary decodedExpected = decodeSummary(encodedExpected);
            MutationSummary actual = summaryForTable(keyspaceName, tableName);
            Assert.assertEquals(summaryIdSpace(decodedExpected), summaryIdSpace(actual));
        });
    }

    public static void assertOffsetsIsSuperSet(Offsets expectedSuperset, Offsets expectedSubset)
    {
        Offsets diff = Offsets.difference(expectedSubset, expectedSuperset);
        if (!diff.isEmpty())
        {
            String msg = String.format("%s not a super set of %s\n", expectedSuperset, expectedSubset);
            msg = msg + String.format("%s found in expected subset that are not in the expected superset", diff);
            throw new AssertionError(msg);
        }
    }

    public static void assertSummaryIsUnreconciledSuperSet(MutationSummary expectedSuperset, MutationSummary expectedSubset)
    {
        for (int i = 0; i < expectedSubset.size(); i++)
        {
            CoordinatorSummary subset = expectedSubset.get(i);
            CoordinatorSummary superset = expectedSuperset.get(subset.logId());

            if (superset == null)
                throw new AssertionError(String.format("Coordinator summary for %s found in expected subset and not in expected superset", subset.logId()));

            assertOffsetsIsSuperSet(superset.unreconciled, subset.unreconciled);
        }
    }

    public static void assertSummaryIdSpaceIsSuperSet(MutationSummary expectedSuperset, MutationSummary expectedSubset)
    {
        for (int i = 0; i < expectedSubset.size(); i++)
        {
            CoordinatorSummary subset = expectedSubset.get(i);
            CoordinatorSummary superset = expectedSuperset.get(subset.logId());

            if (superset == null)
                throw new AssertionError(String.format("Coordinator summary for %s found in expected subset and not in expected superset", subset.logId()));

            assertOffsetsIsSuperSet(summaryIdSpace(superset), summaryIdSpace(subset));
        }
    }

    public static void assertIdsForTable(IInvokableInstance node, String keyspaceName, String tableName, Set<MutationId> expected)
    {
        MutationSummary summary = summaryForTable(node, keyspaceName, tableName);
        assertSummaryContents(summary, expected);
    }

    public static long numLogReconciliations(IInvokableInstance node)
    {
        return node.callOnInstance(() -> ReadRepairMetrics.trackedReconcile.getCount());
    }

    public static Object[] row(Object... objs)
    {
        return objs;
    }

    public static Object[][] rows(Object[][]... objs)
    {
        return objs;
    }

    public static void fixmeSkipIfTracked(ReplicationType replicationType, String reason)
    {
        Assume.assumeFalse(replicationType.isTracked());
    }

    public static CoordinatorLogId getOnlyLogId(MutationSummary summary)
    {
        Assert.assertEquals(1, summary.size());
        return summary.get(0).logId();
    }
}
