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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

public class TableMetadataTest
{
    @Test
    public void testPartitionKeyAsCQLLiteral()
    {
        String keyspaceName = "keyspace";
        String tableName = "table";
        CompositeType compositeType1 = CompositeType.getInstance(UTF8Type.instance, UTF8Type.instance, UTF8Type.instance);
        TableMetadata metadata1 = TableMetadata.builder(keyspaceName, tableName)
                                               .addPartitionKeyColumn("key", compositeType1)
                                               .build();

        String keyAsCQLLiteral = metadata1.partitionKeyAsCQLLiteral(compositeType1.decompose("test:", "composite!", "type)"));
        assertThat(keyAsCQLLiteral).isEqualTo("('test:', 'composite!', 'type)')");

        CompositeType compositeType2 = CompositeType.getInstance(new TupleType(Arrays.asList(FloatType.instance,
                                                                                             UTF8Type.instance)),
                                                                 IntegerType.instance);
        TableMetadata metadata2 = TableMetadata.builder(keyspaceName, tableName)
                                               .addPartitionKeyColumn("key", compositeType2)
                                               .build();
        String keyAsCQLLiteral2 = metadata2.partitionKeyAsCQLLiteral(compositeType2.decompose(TupleType.buildValue(FloatType.instance.decompose(0.33f),
                                                                                                                   UTF8Type.instance.decompose("tuple test")),
                                                                                              BigInteger.valueOf(10)));
        assertThat(keyAsCQLLiteral2).isEqualTo("((0.33, 'tuple test'), 10)");

        TableMetadata metadata3 = TableMetadata.builder(keyspaceName, tableName).addPartitionKeyColumn("key", UTF8Type.instance).build();
        assertEquals("'non-composite test'", metadata3.partitionKeyAsCQLLiteral(UTF8Type.instance.decompose("non-composite test")));
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
        assertThat(metadata.primaryKeyAsCQLLiteral(UTF8Type.instance.decompose("Test"), Clustering.EMPTY)).isEqualTo("'Test'");

        // two partition key columns, no clustering key
        metadata = TableMetadata.builder(keyspaceName, tableName)
                                .addPartitionKeyColumn("k1", UTF8Type.instance)
                                .addPartitionKeyColumn("k2", Int32Type.instance)
                                .build();
        assertThat(metadata.primaryKeyAsCQLLiteral(CompositeType.getInstance(UTF8Type.instance, Int32Type.instance)
                                                                .decompose("Test", -12), Clustering.EMPTY)).isEqualTo("('Test', -12)");

        // one partition key column, one clustering key column
        metadata = TableMetadata.builder(keyspaceName, tableName)
                                .addPartitionKeyColumn("key", UTF8Type.instance)
                                .addClusteringColumn("clustering", UTF8Type.instance)
                                .build();
        assertThat(metadata.primaryKeyAsCQLLiteral(UTF8Type.instance.decompose("k"),
                                                   Clustering.make(UTF8Type.instance.decompose("Cluster")))).isEqualTo("('k', 'Cluster')");
        assertThat(metadata.primaryKeyAsCQLLiteral(UTF8Type.instance.decompose("k"), Clustering.EMPTY)).isEqualTo("'k'");
        assertThat(metadata.primaryKeyAsCQLLiteral(UTF8Type.instance.decompose("k"), Clustering.STATIC_CLUSTERING)).isEqualTo("'k'");

        // one partition key column, two clustering key columns
        metadata = TableMetadata.builder(keyspaceName, tableName)
                                .addPartitionKeyColumn("key", UTF8Type.instance)
                                .addClusteringColumn("c1", UTF8Type.instance)
                                .addClusteringColumn("c2", UTF8Type.instance)
                                .build();
        assertThat(metadata.primaryKeyAsCQLLiteral(UTF8Type.instance.decompose("k"),
                                                   Clustering.make(UTF8Type.instance.decompose("c1"),
                                                                   UTF8Type.instance.decompose("c2")))).isEqualTo("('k', 'c1', 'c2')");
        assertThat(metadata.primaryKeyAsCQLLiteral(UTF8Type.instance.decompose("k"), Clustering.EMPTY)).isEqualTo("'k'");
        assertThat(metadata.primaryKeyAsCQLLiteral(UTF8Type.instance.decompose("k"), Clustering.STATIC_CLUSTERING)).isEqualTo("'k'");

        // two partition key columns, two clustering key columns
        CompositeType composite = CompositeType.getInstance(Int32Type.instance, BooleanType.instance);
        metadata = TableMetadata.builder(keyspaceName, tableName)
                                .addPartitionKeyColumn("k1", Int32Type.instance)
                                .addPartitionKeyColumn("k2", BooleanType.instance)
                                .addClusteringColumn("c1", UTF8Type.instance)
                                .addClusteringColumn("c2", UTF8Type.instance)
                                .build();
        assertThat(metadata.primaryKeyAsCQLLiteral(composite.decompose(0, true),
                                                   Clustering.make(UTF8Type.instance.decompose("Cluster_1"),
                                                                   UTF8Type.instance.decompose("Cluster_2"))))
        .isEqualTo("(0, true, 'Cluster_1', 'Cluster_2')");

        assertThat(metadata.primaryKeyAsCQLLiteral(composite.decompose(1, true), Clustering.EMPTY)).isEqualTo("(1, true)");
        assertThat(metadata.primaryKeyAsCQLLiteral(composite.decompose(2, true), Clustering.STATIC_CLUSTERING)).isEqualTo("(2, true)");
    }
}
