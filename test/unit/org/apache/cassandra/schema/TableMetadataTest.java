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

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.junit.Test;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UTF8Type;

import static org.junit.Assert.assertEquals;

public class TableMetadataTest
{
    @Test
    public void testPartitionKeyAsCQLLiteral()
    {
        String keyspaceName = "keyspace";
        String tableName = "table";

        // composite type
        CompositeType type1 = CompositeType.getInstance(UTF8Type.instance, UTF8Type.instance, UTF8Type.instance);
        TableMetadata metadata1 = TableMetadata.builder(keyspaceName, tableName)
                                               .addPartitionKeyColumn("key", type1)
                                               .build();
        assertEquals("('test:', 'composite!', 'type)')",
                     metadata1.partitionKeyAsCQLLiteral(type1.decompose("test:", "composite!", "type)")));

        // composite type with tuple
        CompositeType type2 = CompositeType.getInstance(new TupleType(Arrays.asList(FloatType.instance, UTF8Type.instance)),
                                                        IntegerType.instance);
        TableMetadata metadata2 = TableMetadata.builder(keyspaceName, tableName)
                                               .addPartitionKeyColumn("key", type2)
                                               .build();
        ByteBuffer tupleValue = TupleType.buildValue(new ByteBuffer[]{ FloatType.instance.decompose(0.33f),
                                                                       UTF8Type.instance.decompose("tuple test") });
        assertEquals("((0.33, 'tuple test'), 10)",
                     metadata2.partitionKeyAsCQLLiteral(type2.decompose(tupleValue, BigInteger.valueOf(10))));

        // plain type
        TableMetadata metadata3 = TableMetadata.builder(keyspaceName, tableName)
                                               .addPartitionKeyColumn("key", UTF8Type.instance).build();
        assertEquals("'non-composite test'",
                     metadata3.partitionKeyAsCQLLiteral(UTF8Type.instance.decompose("non-composite test")));
    }

    @Test
    public void testPrimaryKeyAsCQLLiteral()
    {
        String keyspaceName = "keyspace";
        String tableName = "table";

        TableMetadata metadata;

        // one partition key column, no clustering key
        metadata = TableMetadata.builder(keyspaceName, tableName)
                                .addPartitionKeyColumn("key", UTF8Type.instance)
                                .build();
        assertEquals("'Test'", metadata.primaryKeyAsCQLLiteral(UTF8Type.instance.decompose("Test"), Clustering.EMPTY));

        // two partition key columns, no clustering key
        metadata = TableMetadata.builder(keyspaceName, tableName)
                                .addPartitionKeyColumn("k1", UTF8Type.instance)
                                .addPartitionKeyColumn("k2", Int32Type.instance)
                                .build();
        assertEquals("('Test', -12)",
                     metadata.primaryKeyAsCQLLiteral(CompositeType.getInstance(UTF8Type.instance, Int32Type.instance)
                                                                  .decompose("Test", -12), Clustering.EMPTY));

        // one partition key column, one clustering key column
        metadata = TableMetadata.builder(keyspaceName, tableName)
                                .addPartitionKeyColumn("key", UTF8Type.instance)
                                .addClusteringColumn("clustering", UTF8Type.instance)
                                .build();
        assertEquals("('k', 'Cluster')",
                     metadata.primaryKeyAsCQLLiteral(UTF8Type.instance.decompose("k"),
                                                     Clustering.make(UTF8Type.instance.decompose("Cluster"))));
        assertEquals("'k'",
                     metadata.primaryKeyAsCQLLiteral(UTF8Type.instance.decompose("k"), Clustering.EMPTY));
        assertEquals("'k'",
                     metadata.primaryKeyAsCQLLiteral(UTF8Type.instance.decompose("k"), Clustering.STATIC_CLUSTERING));

        // one partition key column, two clustering key columns
        metadata = TableMetadata.builder(keyspaceName, tableName)
                                .addPartitionKeyColumn("key", UTF8Type.instance)
                                .addClusteringColumn("c1", UTF8Type.instance)
                                .addClusteringColumn("c2", UTF8Type.instance)
                                .build();
        assertEquals("('k', 'c1', 'c2')",
                     metadata.primaryKeyAsCQLLiteral(UTF8Type.instance.decompose("k"),
                                                     Clustering.make(UTF8Type.instance.decompose("c1"),
                                                                     UTF8Type.instance.decompose("c2"))));
        assertEquals("'k'",
                     metadata.primaryKeyAsCQLLiteral(UTF8Type.instance.decompose("k"), Clustering.EMPTY));
        assertEquals("'k'",
                     metadata.primaryKeyAsCQLLiteral(UTF8Type.instance.decompose("k"), Clustering.STATIC_CLUSTERING));

        // two partition key columns, two clustering key columns
        CompositeType composite = CompositeType.getInstance(Int32Type.instance, BooleanType.instance);
        metadata = TableMetadata.builder(keyspaceName, tableName)
                                .addPartitionKeyColumn("k1", Int32Type.instance)
                                .addPartitionKeyColumn("k2", BooleanType.instance)
                                .addClusteringColumn("c1", UTF8Type.instance)
                                .addClusteringColumn("c2", UTF8Type.instance)
                                .build();
        assertEquals("(0, true, 'Cluster_1', 'Cluster_2')",
                     metadata.primaryKeyAsCQLLiteral(composite.decompose(0, true),
                                                     Clustering.make(UTF8Type.instance.decompose("Cluster_1"),
                                                                     UTF8Type.instance.decompose("Cluster_2"))));
        assertEquals("(1, true)",
                     metadata.primaryKeyAsCQLLiteral(composite.decompose(1, true), Clustering.EMPTY));
        assertEquals("(2, true)",
                     metadata.primaryKeyAsCQLLiteral(composite.decompose(2, true), Clustering.STATIC_CLUSTERING));
    }
}
