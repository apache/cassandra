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

import org.junit.Test;

import org.apache.cassandra.cql3.functions.masking.ColumnMask;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor.SerializableCallable;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertTrue;

/**
 * Tests that columns with an attached dynamic data mask survive a cluster metadata snapshot.
 * <p>
 * Ordinary schema changes are propagated as transformations that every node replays locally, so they never exercise
 * {@link ColumnMask.Serializer}. The serializer is only reached once the schema is embedded in a serialized
 * {@link ClusterMetadata}, which happens for cluster metadata snapshots. Those snapshots are read back when a node
 * replays its persisted log at startup, and when a lagging peer catches up from the CMS.
 */
public class ColumnMaskMetadataSnapshotTest extends TestBaseImpl
{
    private static final String TABLE = "masked";

    private static final String CREATE_TABLE =
    "CREATE TABLE %s." + TABLE + " (" +
    "k int PRIMARY KEY, " +
    "no_args text MASKED WITH DEFAULT, " +           // masking function without partial arguments
    "with_args text MASKED WITH mask_inner(2, 1), " + // masking function with partial arguments
    "with_null text MASKED WITH mask_inner(null, 1))"; // masking function with a null partial argument

    /**
     * Tests that the masks of a table can be read back from a cluster metadata snapshot.
     */
    @Test
    public void testMasksSurviveMetadataSnapshot() throws IOException
    {
        try (Cluster cluster = init(build(1)))
        {
            cluster.schemaChange(withKeyspace(CREATE_TABLE));

            IInvokableInstance node = cluster.get(1);
            node.nodetoolResult("cms", "snapshot").asserts().success();

            assertTrue("the masks in the cluster metadata snapshot should match the in-memory schema",
                       node.callOnInstance(snapshotMasksMatchCurrent(KEYSPACE)));
        }
    }

    /**
     * Tests that a node with masked columns can restart once its cluster metadata has been snapshotted, since startup
     * replays the persisted log on top of the latest snapshot.
     */
    @Test
    public void testRestartAfterMetadataSnapshot() throws Throwable
    {
        try (Cluster cluster = init(build(1)))
        {
            cluster.schemaChange(withKeyspace(CREATE_TABLE));

            IInvokableInstance node = cluster.get(1);
            node.nodetoolResult("cms", "snapshot").asserts().success();

            node.shutdown().get();
            node.startup();

            // the masks should still be attached to the columns rebuilt from the snapshot
            assertTrue("the masked columns should still carry their masks after a restart",
                       node.callOnInstance(currentColumnsAreMasked(KEYSPACE)));
        }
    }

    private static Cluster build(int nodeCount) throws IOException
    {
        return Cluster.build()
                      .withNodes(nodeCount)
                      .withConfig(conf -> conf.set("dynamic_data_masking_enabled", "true"))
                      .start();
    }

    /**
     * @return a task comparing the masks stored in the latest cluster metadata snapshot to the in-memory ones,
     * returning {@code true} if every masked column has an identical mask in the snapshot
     */
    private static SerializableCallable<Boolean> snapshotMasksMatchCurrent(String keyspace)
    {
        String table = TABLE; // capture as a local, so the lambda doesn't read a static across the classloader boundary
        return () -> {
            ClusterMetadata snapshot = ClusterMetadataService.instance().snapshotManager().getLatestSnapshot();
            if (snapshot == null)
                return false;

            TableMetadata fromSnapshot = snapshot.schema.getKeyspaceMetadata(keyspace).tables.getNullable(table);
            TableMetadata current = ClusterMetadata.current().schema.getKeyspaceMetadata(keyspace).tables.getNullable(table);
            if (fromSnapshot == null || current == null)
                return false;

            for (ColumnMetadata expected : current.columns())
            {
                ColumnMask expectedMask = expected.getMask();
                if (expectedMask == null)
                    continue;

                ColumnMetadata actual = fromSnapshot.getColumn(expected.name.bytes);
                if (actual == null || !expectedMask.equals(actual.getMask()))
                    return false;
            }
            return true;
        };
    }

    /**
     * @return a task returning {@code true} if the in-memory schema still has a mask attached to every masked column
     */
    private static SerializableCallable<Boolean> currentColumnsAreMasked(String keyspace)
    {
        String tableName = TABLE; // capture as a local, see snapshotMasksMatchCurrent
        return () -> {
            TableMetadata table = ClusterMetadata.current().schema.getKeyspaceMetadata(keyspace).tables.getNullable(tableName);
            if (table == null)
                return false;

            for (String column : new String[]{ "no_args", "with_args", "with_null" })
            {
                ColumnMetadata metadata = table.getColumn(ByteBufferUtil.bytes(column));
                if (metadata == null || metadata.getMask() == null)
                    return false;
            }
            return true;
        };
    }
}
