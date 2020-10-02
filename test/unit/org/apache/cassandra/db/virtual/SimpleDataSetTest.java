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

package org.apache.cassandra.db.virtual;

import java.nio.ByteBuffer;
import java.util.Iterator;

import org.junit.Test;

import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.schema.TableMetadata;

import static org.junit.Assert.assertEquals;

public class SimpleDataSetTest
{
    @Test
    public void shouldDecomposeCompositePartitionKey()
    {
        TableMetadata metadata = TableMetadata.builder("simple_data_set", "composite_pk")
                                              .kind(TableMetadata.Kind.VIRTUAL)
                                              .addPartitionKeyColumn("k1", UTF8Type.instance)
                                              .addPartitionKeyColumn("k2", UTF8Type.instance)
                                              .addRegularColumn("value", Int32Type.instance)
                                              .partitioner(Murmur3Partitioner.instance)
                                              .build();
        
        SimpleDataSet data = new SimpleDataSet(metadata).row("first", "second");
        ByteBuffer expected = ((CompositeType) metadata.partitionKeyType).decompose("first", "second");
        
        Iterator<AbstractVirtualTable.Partition> partitions = data.getPartitions(DataRange.allData(Murmur3Partitioner.instance));
        assertEquals(expected, partitions.next().key().getKey());
    }
}
