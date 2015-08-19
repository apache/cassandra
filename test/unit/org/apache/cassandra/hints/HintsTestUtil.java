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
package org.apache.cassandra.hints;

import java.util.UUID;

import com.google.common.collect.Iterators;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.AbstractBTreePartition;
import org.apache.cassandra.db.partitions.PartitionUpdate;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

final class HintsTestUtil
{
    static void assertMutationsEqual(Mutation expected, Mutation actual)
    {
        assertEquals(expected.key(), actual.key());
        assertEquals(expected.getPartitionUpdates().size(), actual.getPartitionUpdates().size());

        for (UUID id : expected.getColumnFamilyIds())
            assertPartitionsEqual(expected.getPartitionUpdate(id), actual.getPartitionUpdate(id));
    }

    static void assertPartitionsEqual(AbstractBTreePartition expected, AbstractBTreePartition actual)
    {
        assertEquals(expected.partitionKey(), actual.partitionKey());
        assertEquals(expected.deletionInfo(), actual.deletionInfo());
        assertEquals(expected.columns(), actual.columns());
        assertTrue(Iterators.elementsEqual(expected.iterator(), actual.iterator()));
    }

    static void assertHintsEqual(Hint expected, Hint actual)
    {
        assertEquals(expected.mutation.getKeyspaceName(), actual.mutation.getKeyspaceName());
        assertEquals(expected.mutation.key(), actual.mutation.key());
        assertEquals(expected.mutation.getColumnFamilyIds(), actual.mutation.getColumnFamilyIds());
        for (PartitionUpdate partitionUpdate : expected.mutation.getPartitionUpdates())
            assertPartitionsEqual(partitionUpdate, actual.mutation.getPartitionUpdate(partitionUpdate.metadata().cfId));
        assertEquals(expected.creationTime, actual.creationTime);
        assertEquals(expected.gcgs, actual.gcgs);
    }
}
