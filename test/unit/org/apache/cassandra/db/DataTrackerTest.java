/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.junit.Test;

import junit.framework.Assert;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.utils.ByteBufferUtil;

public class DataTrackerTest extends SchemaLoader
{
    private static final String KEYSPACE = "Keyspace1";
    private static final String CF = "Standard1";

    @Test
    public void testCompactOnlyCorrectInstance()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        cfs.truncateBlocking();
        for (int j = 0; j < 100; j ++)
        {
            ByteBuffer key = ByteBufferUtil.bytes(String.valueOf(j));
            Mutation rm = new Mutation(KEYSPACE, key);
            rm.add(CF, Util.cellname("0"), ByteBufferUtil.EMPTY_BYTE_BUFFER, j);
            rm.apply();
        }
        cfs.forceBlockingFlush();
        Set<SSTableReader> sstables = new HashSet<>(cfs.getSSTables());
        assert sstables.size() == 1;
        SSTableReader reader = Iterables.getFirst(sstables, null);
        SSTableReader reader2 = reader.cloneAsShadowed(new Runnable() { public void run() { } });
        Assert.assertFalse(cfs.getDataTracker().markCompacting(ImmutableList.of(reader2)));
        Assert.assertTrue(cfs.getDataTracker().markCompacting(ImmutableList.of(reader)));
        cfs.getDataTracker().replaceWithNewInstances(ImmutableList.of(reader), ImmutableList.of(reader2));
        cfs.getDataTracker().unmarkCompacting(ImmutableList.of(reader));
        cfs.truncateBlocking();
    }

}
