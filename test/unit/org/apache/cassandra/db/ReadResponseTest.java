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
package org.apache.cassandra.db;

import java.util.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.db.partitions.ImmutableBTreePartition;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.IPartitioner;

import static org.junit.Assert.assertEquals;

public class ReadResponseTest extends CQLTester
{
    private IPartitioner partitionerToRestore;

    @Before
    public void setupPartitioner()
    {
        // Using an ordered partitioner to be able to predict keys order in the following tests.
        partitionerToRestore = DatabaseDescriptor.setPartitionerUnsafe(ByteOrderedPartitioner.instance);
    }

    @After
    public void resetPartitioner()
    {
        DatabaseDescriptor.setPartitionerUnsafe(partitionerToRestore);
    }

    @Test
    public void testLegacyResponseSkipWrongBounds()
    {
        createTable("CREATE TABLE %s (k text PRIMARY KEY)");

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        // Test that if a legacy response contains keys at the boundary of the requested key range that shouldn't be present, those
        // are properly skipped. See CASSANDRA-9857 for context.

        List<ImmutableBTreePartition> responses = Arrays.asList(makePartition(cfs.metadata, "k1"),
                                                                makePartition(cfs.metadata, "k2"),
                                                                makePartition(cfs.metadata, "k3"));
        ReadResponse.LegacyRemoteDataResponse response = new ReadResponse.LegacyRemoteDataResponse(responses);

        assertPartitions(response.makeIterator(Util.cmd(cfs).fromKeyExcl("k1").toKeyExcl("k3").build()), "k2");
        assertPartitions(response.makeIterator(Util.cmd(cfs).fromKeyExcl("k0").toKeyExcl("k3").build()), "k1", "k2");
        assertPartitions(response.makeIterator(Util.cmd(cfs).fromKeyExcl("k1").toKeyExcl("k4").build()), "k2", "k3");

        assertPartitions(response.makeIterator(Util.cmd(cfs).fromKeyIncl("k1").toKeyExcl("k3").build()), "k1", "k2");
        assertPartitions(response.makeIterator(Util.cmd(cfs).fromKeyIncl("k1").toKeyExcl("k4").build()), "k1", "k2", "k3");
    }

    private void assertPartitions(UnfilteredPartitionIterator actual, String... expectedKeys)
    {
        int i = 0;
        while (i < expectedKeys.length && actual.hasNext())
        {
            String actualKey = AsciiType.instance.getString(actual.next().partitionKey().getKey());
            assertEquals(expectedKeys[i++], actualKey);
        }

        if (i < expectedKeys.length)
            throw new AssertionError("Got less results than expected: " + expectedKeys[i] + " is not in the result");
        if (actual.hasNext())
            throw new AssertionError("Got more results than expected: first unexpected key is " + AsciiType.instance.getString(actual.next().partitionKey().getKey()));
    }

    private static ImmutableBTreePartition makePartition(CFMetaData metadata, String key)
    {
        return ImmutableBTreePartition.create(UnfilteredRowIterators.noRowsIterator(metadata, Util.dk(key), Rows.EMPTY_STATIC_ROW, new DeletionTime(0, 0), false));
    }
}
