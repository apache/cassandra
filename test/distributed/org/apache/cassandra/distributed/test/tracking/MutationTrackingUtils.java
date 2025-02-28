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
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.google.common.primitives.Ints;

import org.junit.Assert;
import org.junit.Assume;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.MutationId;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.metrics.ReadRepairMetrics;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.replication.simple.SimpleMutationSummary;
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

    public static SimpleMutationSummary summaryForKey(String keyspaceName, String tableName, DecoratedKey dk)
    {
        TableMetadata table = Schema.instance.getTableMetadata(keyspaceName, tableName);
        return  (SimpleMutationSummary) MutationTrackingService.instance().summaryForKey(table.id, dk);
    }

    public static SimpleMutationSummary summaryForKey(String keyspaceName, String tableName, int key)
    {
        return  summaryForKey(keyspaceName, tableName, Murmur3Partitioner.instance.decorateKey(ByteBufferUtil.bytes(key)));
    }

    public static SimpleMutationSummary summaryForRange(String keyspaceName, String tableName, Range<Token> range)
    {
        TableMetadata table = Schema.instance.getTableMetadata(keyspaceName, tableName);
        return  (SimpleMutationSummary) MutationTrackingService.instance().summaryForRange(table.id, range);
    }

    public static Set<MutationId> getIdsForKey(IInvokableInstance node, String keyspaceName, String tableName, int key)
    {
        byte[][] encodedIds = node.callOnInstance(() -> {
            DecoratedKey dk = Murmur3Partitioner.instance.decorateKey(ByteBufferUtil.bytes(key));
            SimpleMutationSummary summary = summaryForKey(keyspaceName, tableName, dk);
            SortedSet<MutationId> ids = summary.ids.get(dk);
            if (ids == null || ids.isEmpty())
                return new byte[][] {};

            byte[][] result = new byte[ids.size()][];
            int idx = 0;
            for (MutationId id : ids)
            {
                result[idx++] = encodeId(id);
            }
            return result;
        });

        SortedSet<MutationId> result = new TreeSet<>();
        for (byte[] encodedId : encodedIds)
            result.add(decodeId(encodedId));
        return result;
    }

    public static Set<MutationId> getIdsForTable(IInvokableInstance node, String keyspaceName, String tableName)
    {
        byte[][] encodedIds = node.callOnInstance(() -> {
            Range<Token> range = new Range<>(Murmur3Partitioner.instance.getMinimumToken(), Murmur3Partitioner.instance.getMinimumToken());
            SimpleMutationSummary summary = summaryForRange(keyspaceName, tableName, range);


            TreeSet<MutationId> ids = new TreeSet<>();
            summary.ids.values().forEach(ids::addAll);
            if (ids == null || ids.isEmpty())
                return new byte[][] {};

            byte[][] result = new byte[ids.size()][];
            int idx = 0;
            for (MutationId id : ids)
            {
                result[idx++] = encodeId(id);
            }
            return result;
        });

        SortedSet<MutationId> result = new TreeSet<>();
        for (byte[] encodedId : encodedIds)
            result.add(decodeId(encodedId));
        return result;
    }


    public static void assertIdsForKey(IInvokableInstance node, String keyspaceName, String tableName, int key, Set<MutationId> expected)
    {
        Set<MutationId> actual = getIdsForKey(node, keyspaceName, tableName, key);
        Assert.assertEquals(expected, actual);
    }

    public static void assertIdsForTable(IInvokableInstance node, String keyspaceName, String tableName, Set<MutationId> expected)
    {
        Set<MutationId> actual = getIdsForTable(node, keyspaceName, tableName);
        Assert.assertEquals(expected, actual);
    }

    public static long numLogReconciliations(IInvokableInstance node)
    {
        return node.callOnInstance(() -> ReadRepairMetrics.logReconcile.getCount());
    }

    public static Object[] row(Object... objs)
    {
        return objs;
    }

    public static Object[][] rows(Object[][]... objs)
    {
        return objs;
    }

    public static void fixmeSkipIfLogged(ReplicationType replicationType, String reason)
    {
        Assume.assumeFalse(replicationType.isLogged());

    }
}
