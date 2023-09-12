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

package org.apache.cassandra.index.sai.utils;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

public class AbstractPrimaryKeyTest extends SaiRandomizedTest
{
    static TableMetadata simplePartition = TableMetadata.builder("test", "test")
                                                        .partitioner(Murmur3Partitioner.instance)
                                                        .addPartitionKeyColumn("pk1", Int32Type.instance)
                                                        .build();

    static TableMetadata compositePartition = TableMetadata.builder("test", "test")
                                                           .partitioner(Murmur3Partitioner.instance)
                                                           .addPartitionKeyColumn("pk1", Int32Type.instance)
                                                           .addPartitionKeyColumn("pk2", Int32Type.instance)
                                                           .build();

    static TableMetadata simplePartitionSingleClusteringAsc = TableMetadata.builder("test", "test")
                                                                           .partitioner(Murmur3Partitioner.instance)
                                                                           .addPartitionKeyColumn("pk1", Int32Type.instance)
                                                                           .addClusteringColumn("ck1", UTF8Type.instance)
                                                                           .build();

    static TableMetadata simplePartitionMultipleClusteringAsc = TableMetadata.builder("test", "test")
                                                                             .partitioner(Murmur3Partitioner.instance)
                                                                             .addPartitionKeyColumn("pk1", Int32Type.instance)
                                                                             .addClusteringColumn("ck1", UTF8Type.instance)
                                                                             .addClusteringColumn("ck2", UTF8Type.instance)
                                                                             .build();

    static TableMetadata simplePartitionSingleClusteringDesc = TableMetadata.builder("test", "test")
                                                                            .partitioner(Murmur3Partitioner.instance)
                                                                            .addPartitionKeyColumn("pk1", Int32Type.instance)
                                                                            .addClusteringColumn("ck1", ReversedType.getInstance(UTF8Type.instance))
                                                                            .build();

    static TableMetadata simplePartitionMultipleClusteringDesc = TableMetadata.builder("test", "test")
                                                                              .partitioner(Murmur3Partitioner.instance)
                                                                              .addPartitionKeyColumn("pk1", Int32Type.instance)
                                                                              .addClusteringColumn("ck1", ReversedType.getInstance(UTF8Type.instance))
                                                                              .addClusteringColumn("ck2", ReversedType.getInstance(UTF8Type.instance))
                                                                              .build();

    static TableMetadata compositePartitionSingleClusteringAsc = TableMetadata.builder("test", "test")
                                                                              .partitioner(Murmur3Partitioner.instance)
                                                                              .addPartitionKeyColumn("pk1", Int32Type.instance)
                                                                              .addPartitionKeyColumn("pk2", Int32Type.instance)
                                                                              .addClusteringColumn("ck1", UTF8Type.instance)
                                                                              .build();

    static TableMetadata compositePartitionMultipleClusteringAsc = TableMetadata.builder("test", "test")
                                                                                .partitioner(Murmur3Partitioner.instance)
                                                                                .addPartitionKeyColumn("pk1", Int32Type.instance)
                                                                                .addPartitionKeyColumn("pk2", Int32Type.instance)
                                                                                .addClusteringColumn("ck1", UTF8Type.instance)
                                                                                .addClusteringColumn("ck2", UTF8Type.instance)
                                                                                .build();

    static TableMetadata compositePartitionSingleClusteringDesc = TableMetadata.builder("test", "test")
                                                                               .partitioner(Murmur3Partitioner.instance)
                                                                               .addPartitionKeyColumn("pk1", Int32Type.instance)
                                                                               .addPartitionKeyColumn("pk2", Int32Type.instance)
                                                                               .addClusteringColumn("ck1", ReversedType.getInstance(UTF8Type.instance))
                                                                               .build();

    static TableMetadata compositePartitionMultipleClusteringDesc = TableMetadata.builder("test", "test")
                                                                                 .partitioner(Murmur3Partitioner.instance)
                                                                                 .addPartitionKeyColumn("pk1", Int32Type.instance)
                                                                                 .addPartitionKeyColumn("pk2", Int32Type.instance)
                                                                                 .addClusteringColumn("ck1", ReversedType.getInstance(UTF8Type.instance))
                                                                                 .addClusteringColumn("ck2", ReversedType.getInstance(UTF8Type.instance))
                                                                                 .build();

    static TableMetadata simplePartitionMultipleClusteringMixed = TableMetadata.builder("test", "test")
                                                                               .partitioner(Murmur3Partitioner.instance)
                                                                               .addPartitionKeyColumn("pk1", Int32Type.instance)
                                                                               .addClusteringColumn("ck1", UTF8Type.instance)
                                                                               .addClusteringColumn("ck2", ReversedType.getInstance(UTF8Type.instance))
                                                                               .build();

    static TableMetadata compositePartitionMultipleClusteringMixed = TableMetadata.builder("test", "test")
                                                                                  .partitioner(Murmur3Partitioner.instance)
                                                                                  .addPartitionKeyColumn("pk1", Int32Type.instance)
                                                                                  .addPartitionKeyColumn("pk2", Int32Type.instance)
                                                                                  .addClusteringColumn("ck1", UTF8Type.instance)
                                                                                  .addClusteringColumn("ck2", ReversedType.getInstance(UTF8Type.instance))
                                                                                  .build();

    void assertByteComparison(PrimaryKey a, PrimaryKey b, int expected)
    {
        assertEquals(expected, ByteComparable.compare(v -> a.asComparableBytes(v),
                                                      v -> b.asComparableBytes(v),
                                                      ByteComparable.Version.OSS41));
    }

    void assertCompareToAndEquals(PrimaryKey a, PrimaryKey b, int expected)
    {
        if (expected > 0)
        {
            assertTrue(a.compareTo(b) > 0);
            assertNotEquals(a, b);
        }
        else if (expected < 0)
        {
            assertTrue(a.compareTo(b) < 0);
            assertNotEquals(a, b);
        }
        else
        {
            assertTrue(a.compareTo(b) == 0);
            assertEquals(a, b);
        }
    }

    DecoratedKey makeKey(TableMetadata table, Object...partitionKeys)
    {
        ByteBuffer key;
        if (TypeUtil.isComposite(table.partitionKeyType))
            key = ((CompositeType)table.partitionKeyType).decompose(partitionKeys);
        else
            key = table.partitionKeyType.fromString((String)partitionKeys[0]);
        return table.partitioner.decorateKey(key);
    }

    Clustering makeClustering(TableMetadata table, String...clusteringKeys)
    {
        Clustering clustering;
        if (table.comparator.size() == 0)
            clustering = Clustering.EMPTY;
        else
        {
            ByteBuffer[] values = new ByteBuffer[clusteringKeys.length];
            for (int index = 0; index < table.comparator.size(); index++)
                values[index] = table.comparator.subtype(index).fromString(clusteringKeys[index]);
            clustering = Clustering.make(values);
        }
        return clustering;
    }
}
