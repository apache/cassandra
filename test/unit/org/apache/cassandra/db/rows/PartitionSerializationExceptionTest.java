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

package org.apache.cassandra.db.rows;

import org.junit.Test;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.schema.TableMetadata;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PartitionSerializationExceptionTest
{
    @Test
    public void testMessageWithSimplePartitionKey()
    {
        TableMetadata metadata = TableMetadata.builder("ks", "tbl").addPartitionKeyColumn("pk", UTF8Type.instance).build();

        DecoratedKey key = mock(DecoratedKey.class);
        when(key.getKey()).thenReturn(UTF8Type.instance.decompose("foo"));
        
        @SuppressWarnings("unchecked") 
        BaseRowIterator<Unfiltered> partition = mock(BaseRowIterator.class);
        when(partition.metadata()).thenReturn(metadata);
        when(partition.partitionKey()).thenReturn(key);
        
        PartitionSerializationException pse = new PartitionSerializationException(partition, new RuntimeException());
        assertEquals("Failed to serialize partition key 'foo' on table 'tbl' in keyspace 'ks'.", pse.getMessage());
    }

    @Test
    public void testMessageWithCompositePartitionKey()
    {
        TableMetadata metadata = TableMetadata.builder("ks", "tbl")
                                              .addPartitionKeyColumn("pk1", UTF8Type.instance)
                                              .addPartitionKeyColumn("pk2", UTF8Type.instance).build();

        DecoratedKey key = mock(DecoratedKey.class);
        CompositeType keyType = CompositeType.getInstance(UTF8Type.instance, UTF8Type.instance);
        when(key.getKey()).thenReturn(keyType.decompose("foo", "bar"));

        @SuppressWarnings("unchecked")
        BaseRowIterator<Unfiltered> partition = mock(BaseRowIterator.class);
        when(partition.metadata()).thenReturn(metadata);
        when(partition.partitionKey()).thenReturn(key);

        PartitionSerializationException pse = new PartitionSerializationException(partition, new RuntimeException());
        assertEquals("Failed to serialize partition key 'foo:bar' on table 'tbl' in keyspace 'ks'.", pse.getMessage());
    }
}
