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
package org.apache.cassandra.db.partition;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.UpdateBuilder;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class PartitionUpdateIsEmptyTest extends CQLTester
{
    private static final int[] VERSIONS =
        MessagingService.Version.supportedVersions().stream().mapToInt(version -> version.value).toArray();

    private TableMetadata metadata;
    private ByteBuffer rowKey;

    @Before
    public void setUpTable()
    {
        createTable("CREATE TABLE %s (k int, c int, s int static, v int, PRIMARY KEY (k, c))");
        metadata = currentTableMetadata();
        rowKey = ByteBufferUtil.bytes(1);
    }

    private PartitionUpdate emptyUpdate()
    {
        return PartitionUpdate.emptyUpdate(metadata, metadata.partitioner.decorateKey(rowKey));
    }

    private PartitionUpdate rowUpdate()
    {
        return UpdateBuilder.create(metadata, 1).newRow(1).add("v", 7).build();
    }

    private PartitionUpdate staticUpdate()
    {
        return UpdateBuilder.create(metadata, 1).newRow().add("s", 7).build();
    }

    private PartitionUpdate partitionDeletion()
    {
        return PartitionUpdate.simpleBuilder(metadata, 1).delete().build();
    }

    private PartitionUpdate rangeTombstone()
    {
        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(metadata, 1);
        builder.addRangeTombstone().start(1).end(5);
        return builder.build();
    }

    @Test
    public void matchesFullDeserialization() throws IOException
    {
        Assert.assertTrue(emptyUpdate().isEmpty());
        Assert.assertFalse(rowUpdate().isEmpty());

        PartitionUpdate[] updates = { emptyUpdate(), rowUpdate(), staticUpdate(), partitionDeletion(), rangeTombstone() };
        for (int i = 0; i < updates.length; i++)
        {
            for (int version : VERSIONS)
            {
                ByteBuffer bytes = PartitionUpdate.toBytes(updates[i], version);
                boolean expected = PartitionUpdate.fromBytes(bytes.duplicate(), version).isEmpty();
                boolean actual = PartitionUpdate.PartitionUpdateSerializer.isEmpty(bytes, rowKey, metadata.id.asUUID(), version);
                Assert.assertEquals("update " + i + " at messaging version " + version, expected, actual);
            }
        }
    }

    @Test
    public void rejectsOtherTable()
    {
        ByteBuffer bytes = PartitionUpdate.toBytes(rowUpdate(), MessagingService.current_version);
        assertThatThrownBy(() -> PartitionUpdate.PartitionUpdateSerializer.isEmpty(bytes, rowKey, UUID.randomUUID(), MessagingService.current_version))
        .isInstanceOf(IOException.class);
    }

    @Test
    public void rejectsOtherKey()
    {
        ByteBuffer bytes = PartitionUpdate.toBytes(rowUpdate(), MessagingService.current_version);
        assertThatThrownBy(() -> PartitionUpdate.PartitionUpdateSerializer.isEmpty(bytes, ByteBufferUtil.bytes(2), metadata.id.asUUID(), MessagingService.current_version))
        .isInstanceOf(IOException.class);
    }

    @Test
    public void rejectsWrongVersion()
    {
        ByteBuffer oldest = PartitionUpdate.toBytes(rowUpdate(), MessagingService.VERSION_40);
        assertThatThrownBy(() -> PartitionUpdate.PartitionUpdateSerializer.isEmpty(oldest, rowKey, metadata.id.asUUID(), MessagingService.current_version))
        .isInstanceOf(IOException.class);

        ByteBuffer newest = PartitionUpdate.toBytes(rowUpdate(), MessagingService.current_version);
        assertThatThrownBy(() -> PartitionUpdate.PartitionUpdateSerializer.isEmpty(newest, rowKey, metadata.id.asUUID(), MessagingService.VERSION_40))
        .isInstanceOf(IOException.class);
    }
}
