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

package org.apache.cassandra.schema;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeVersion;

public class TableMetadataSerDeTest extends TestBaseImpl
{
    @Test
    public void testCASSANDRA_19954() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(1)
                                        .start())
        {
            cluster.coordinator(1).execute("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};",
                                           ConsistencyLevel.ALL);

            cluster.coordinator(1).execute("CREATE TABLE ks.tbl (pk int,\n" +
                                           "    ck int,\n" +
                                           "    v1 int,\n" +
                                           "    v2 text,\n" +
                                           "    v3 text,\n" +
                                           "    v4 text,\n" +
                                           "    v5 text,\n" +
                                           "    PRIMARY KEY (pk, ck))",
                                           ConsistencyLevel.ALL);

            assertTableState(cluster, true, true);

            cluster.coordinator(1).execute("ALTER TABLE ks.tbl WITH allow_auto_snapshot = false", ConsistencyLevel.ALL);
            assertTableState(cluster, false, true);

            cluster.coordinator(1).execute("ALTER TABLE ks.tbl WITH allow_auto_snapshot = true", ConsistencyLevel.ALL);
            assertTableState(cluster, true, true);

            cluster.coordinator(1).execute("ALTER TABLE ks.tbl WITH incremental_backups = false", ConsistencyLevel.ALL);
            assertTableState(cluster, true, false);

            cluster.coordinator(1).execute("ALTER TABLE ks.tbl WITH incremental_backups = true", ConsistencyLevel.ALL);
            assertTableState(cluster, true, true);

            cluster.coordinator(1).execute("ALTER TABLE ks.tbl WITH incremental_backups = false AND allow_auto_snapshot = false", ConsistencyLevel.ALL);
            assertTableState(cluster, false, false);
        }
    }

    private void assertTableState(Cluster cluster, boolean expectedAllowAutoSnapshot, boolean expectedIncrementalBackups)
    {
        cluster.get(1).acceptsOnInstance((IIsolatedExecutor.SerializableBiConsumer<Boolean, Boolean>) (snapshots, backups) -> {
            TableMetadata tableMetadata = ClusterMetadata.current().schema.getKeyspaceMetadata("ks").getTableOrViewNullable("tbl");

            Assert.assertNotNull(tableMetadata);
            Assert.assertEquals(snapshots, tableMetadata.params.allowAutoSnapshot);
            Assert.assertEquals(backups, tableMetadata.params.incrementalBackups);

            ByteBuffer out = null;
            try (DataOutputBuffer dob = new DataOutputBuffer())
            {
                TableMetadata.serializer.serialize(tableMetadata, dob, NodeVersion.CURRENT_METADATA_VERSION);
                out = dob.buffer();
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }

            Assert.assertEquals(out.limit(), TableMetadata.serializer.serializedSize(tableMetadata, NodeVersion.CURRENT_METADATA_VERSION));
            TableMetadata rt = null;
            try
            {
                rt = TableMetadata.serializer.deserialize(new DataInputBuffer(out, true), Types.builder().build(), UserFunctions.builder().build(), NodeVersion.CURRENT_METADATA_VERSION);
            }
            catch (IOException e)
            {
                e.printStackTrace();
                throw new RuntimeException(e);
            }

            Assert.assertEquals(rt, tableMetadata);
        }).accept(expectedAllowAutoSnapshot, expectedIncrementalBackups);
    }

    @Test
    public void droppedColumnsTest() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(1)
                                        .start())
        {
            cluster.coordinator(1).execute("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};",
                                           ConsistencyLevel.ALL);

            cluster.coordinator(1).execute("CREATE TABLE ks.tbl (pk int,\n" +
                                           "    ck int,\n" +
                                           "    v1 int,\n" +
                                           "    v2 text,\n" +
                                           "    v3 text,\n" +
                                           "    v4 text,\n" +
                                           "    v5 text,\n" +
                                           "    PRIMARY KEY (pk, ck))",
                                           ConsistencyLevel.ALL);

            cluster.coordinator(1).execute("ALTER TABLE ks.tbl drop v1",
                                           ConsistencyLevel.ALL);
            cluster.coordinator(1).execute("ALTER TABLE ks.tbl drop v2",
                                           ConsistencyLevel.ALL);
            cluster.coordinator(1).execute("ALTER TABLE ks.tbl drop v3",
                                           ConsistencyLevel.ALL);

            cluster.get(1).runOnInstance(() -> {
                TableMetadata tableMetadata = ClusterMetadata.current().schema.getKeyspaceMetadata("ks").getTableOrViewNullable("tbl");

                ByteBuffer out = null;
                try (DataOutputBuffer dob = new DataOutputBuffer())
                {
                    TableMetadata.serializer.serialize(tableMetadata, dob, NodeVersion.CURRENT_METADATA_VERSION);
                    out = dob.buffer();
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }

                Assert.assertEquals(out.limit(), TableMetadata.serializer.serializedSize(tableMetadata, NodeVersion.CURRENT_METADATA_VERSION));
                TableMetadata rt = null;
                try
                {
                    rt = TableMetadata.serializer.deserialize(new DataInputBuffer(out, true), Types.builder().build(), UserFunctions.builder().build(), NodeVersion.CURRENT_METADATA_VERSION);
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
                Assert.assertEquals(rt, tableMetadata);
            });
        }
    }

    @Test
    public void reversedTypeTest() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(1)
                                        .start())
        {
            cluster.coordinator(1).execute("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};",
                                           ConsistencyLevel.ALL);
            StringBuilder sb = new StringBuilder();
            sb.append("CREATE TABLE ks.tbl (pk int");
            int counter = 1;
            List<String> asc = new ArrayList<>();
            List<String> desc = new ArrayList<>();
            for (CQL3Type.Native value : CQL3Type.Native.values())
            {
                if (value == CQL3Type.Native.EMPTY || value == CQL3Type.Native.COUNTER || value == CQL3Type.Native.DURATION)
                    continue;
                String name = "ck" + (counter++);
                sb.append(", ").append(name).append(" ").append(value.toString());
                asc.add(name);
                name = "ck" + (counter++);
                sb.append(", ").append(name).append(" ").append(value.toString());
                desc.add(name);
            }
            for (CQL3Type.Native value : CQL3Type.Native.values())
            {
                if (value == CQL3Type.Native.EMPTY || value == CQL3Type.Native.COUNTER)
                    continue;
                sb.append(", static").append(counter++).append(" ").append(value.toString()).append(" static");
            }
            for (CQL3Type.Native value : CQL3Type.Native.values())
            {
                if (value == CQL3Type.Native.EMPTY || value == CQL3Type.Native.COUNTER)
                    continue;
                sb.append(", regular").append(counter++).append(" ").append(value.toString());
            }

            sb.append(", primary key (pk");
            for (String s : asc)
                sb.append(", ").append(s);
            for (String s : desc)
                sb.append(", ").append(s);
            sb.append("))");
            sb.append(" WITH CLUSTERING ORDER BY (");
            for (int i = 0; i < asc.size(); i++)
            {
                if (i > 0)
                    sb.append(", ");
                sb.append(asc.get(i)).append(" ASC");
            }
            for (int i = 0; i < desc.size(); i++)
            {
                sb.append(", ");
                sb.append(desc.get(i)).append(" DESC");
            }
            sb.append(");");
            System.out.println("sb.toString() = " + sb.toString());
            cluster.coordinator(1).execute(sb.toString(),
                                           ConsistencyLevel.ALL);
            cluster.get(1).runOnInstance(() -> {
                TableMetadata before = ClusterMetadata.current().schema.getKeyspaceMetadata("ks").getTableOrViewNullable("tbl");

                ByteBuffer out = null;
                try (DataOutputBuffer dob = new DataOutputBuffer())
                {
                    TableMetadata.serializer.serialize(before, dob, NodeVersion.CURRENT_METADATA_VERSION);
                    out = dob.buffer();
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }

                Assert.assertEquals(out.limit(), TableMetadata.serializer.serializedSize(before, NodeVersion.CURRENT_METADATA_VERSION));
                TableMetadata after = null;
                try
                {
                    after = TableMetadata.serializer.deserialize(new DataInputBuffer(out, true), Types.builder().build(), UserFunctions.builder().build(), NodeVersion.CURRENT_METADATA_VERSION);
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }

                Assert.assertEquals(Tables.diff(Tables.of(before), Tables.of(after)).toString(),
                                    after, before);
            });
        }
    }

}
